#!/usr/bin/env python
#Author: Wendell updated by Jeff Jiang
#Updated Date: 2015.03.16

import sys
sys.path.append("/data/migo/athena/lib")

from athena_variable import *
from athena_luigi import *
import time
import datetime

import luigi
import operator
import json
from pymongo import MongoClient

class BatchSave(MigoPeriodHdfs):

    def start(self):
        super(BatchSave, self).start()

        if self.keep_temp and self.hdfsClient.exists(self.dest):
            pass
        else:
            client = MongoClient(MIGO_MONGO_TA_URL)
            db = client.ProjectReport_StarterDIY
            col = db[self.collection]

            col.remove({"ShopID":self.shop_id, "PeriodType": "L" + str(self.period) + "D", "CalDate": datetime.datetime.strptime(self.cal_date, '%Y%m%d')})
            client.close()

    def init_reducer(self):
        self.client = MongoClient(MIGO_MONGO_TA_URL)
        self.db = self.client.ProjectReport_StarterDIY
        self.col = self.db[self.collection]

        self.pool = []

    def add(self, record):
        self.pool.append(record)

        if len(self.pool) >= MIGO_MONGO_TA_BATCH_NUM:
            self.col.insert(self.pool)
            self.pool = []
 
    def end_reducer(self):
        super(BatchSave, self).end_reducer()

        if len(self.pool) > 0:
            self.col.insert(self.pool)
            self.pool = []

        self.client.close()

class CalDashBoardFM(BatchSave):
    """
           JobTask: CalDashBoardFM
        Objectives: Calculate the revenue and frequency for every stores, members and nunmembers respectively.
            Author: Wendell, updated by Jeff Jiang
       UpdatedDate: 2015/03/16
            Source: /user/athena/<shop_id>/data_prepare_recommendation
                    <shop_id> <member_id> <item_id> <transaction_date> <quantity> <amount>
       Destination: None
                    <shop_id> <total_revenue> <member_revenue> <nunmember_revenue> <total_frequency> <member_frequency> <nunmember_frequency>
              Note: Save the results to the MongoDB
             Usage: python new_dashboard.py CalDashBoardFM --use-hadoop --src '/user/athena/data_prepare_recommendation' --dest new_revenue_week/<shop_id>/20140817 --period 7 --cal-date 20140817
    """
    collection = "DashBoardRevenue"

    def mapper(self, line):
        infos = line.strip().split(MIGO_SEPARATOR_LEVEL1)
        store_id, member_id, item_id, ordate, item_qt, amount = infos[0], infos[1], infos[2], infos[3], infos[4], infos[5]

        try:
            if str(member_id) == str(MIGO_ERROR_NUMBER):
                yield store_id, (float(amount), float(0.0), float(amount), int(1), int(0), int(1))
                self.count_success += 1
            else:
                yield store_id, (float(amount), float(amount), float(0.0),  int(1), int(1), int(0))
                self.count_success += 1
        except Exception as e:
            self.count_fail += 1

    def combiner(self, store_id, values):
        try:
            values = list(values)

            yield store_id, (sum([value[0] for value in values]), sum([value[1] for value in values]), sum([value[2] for value in values]), sum([value[3] for value in values]), sum([value[4] for value in values]), sum([value[5] for value in values]))
            self.count_success += len(list(values))
        except Exception as e:
            self.count_fail += len(list(values))

    def reducer(self, key, values):
        try:
            sum_info = [sum(x) for x in zip(*values)]
            total_amount, member_amount, nonmember_amount, total_frequency, member_frequency, nonmember_frequency = sum_info

            StoreID = key
            ins = {
                "ShopID": StoreID.split("^")[0],
                "StoreID": StoreID,
                "PeriodType": "L" + str(self.period) + "D",
                "CalDate": datetime.datetime.strptime(self.cal_date, '%Y%m%d'),
                "TotalAmount": total_amount,
                "MemberAmount": member_amount,
                "NonMemberAmount": nonmember_amount,
                "TotalFrequency": total_frequency,
                "MemberFrequency": member_frequency,
                "NonMemberFrequency": nonmember_frequency
            }

            #self.col.remove({"StoreID": key, "PeriodType": "L" + str(self.period) + "D", "CalDate": datetime.datetime.strptime(self.cal_date, '%Y%m%d')})
            self.add(ins)

            yield key, sum_info

            self.count_success += 1
        except Exception as e:
            self.count_fail += 1

class CalDashBoardItem(BatchSave):
    """
           JobTask: CalDashBoardItem
        Objectives: Calculate the revenue based on the itemID for every stores
            Author: Wendell
      CreationDate: 2015/02/10
            Source: /user/athena/data_prepare_item
                    <shop_id> <item_id> <transaction_date> <quanality> <amount>
       Destination: None
              Note: Save the results to the MongoDB
             Usage: python dashboard.py CalDashBoardItem --use-hadoop --src '/user/athena/data_prepare_item' --period 7 --cal-date 20140817
    """
    top = luigi.IntParameter(default=100)
    collection = "TopItem"

    def mapper(self, line):
        '''
        kgsupermarket^C014  item   3.73571890698e-05
        kgsupermarket^C014  item  2.73952719845e-05
        kgsupermarket^C014  item  0.000118546813315
        kgsupermarket^C014  item 0.000109082992084
        '''
        infos = line.strip().split(MIGO_SEPARATOR_LEVEL1)
        store_id, item_id, ordate, item_qt, amount = infos[0], infos[1], infos[2], infos[3], infos[4]

        try:
            values = float(amount)
            if values > 0:
                yield store_id, (item_id, values, int(item_qt))
                self.count_success += 1
        except Exception as e:
            self.count_fail += 1

    def combiner(self, store_id, values):
        items = {}

        for value in values:
            item, amount, order = value

            items.setdefault(item, [0, 0])
            items[item][0] += amount
            items[item][1] += order

        for item, info in items.items():
            yield store_id, (item, info[0], info[1])

            self.count_success += 1

    def reducer(self, store_id, values):
        ls = sorted(list(values))

        totalamount, totalqty = 0, 0
        for value in ls:
            totalamount += value[1]
            totalqty += value[2]

        if totalamount > 0 and totalqty > 0:
            data = {"qty":{},"amount":{}}
            old, now = None, None

            for x in ls:
                now, amount, qty = x

                if now != old:
                    data["amount"][now] = 0
                    data["qty"][now] = 0
                    old = now

                data["amount"][now] += amount
                data["qty"][now] += qty

            sorted_data = sorted(data["amount"].items(), key=operator.itemgetter(1), reverse=True)
            sorted_data_q = sorted(data["qty"].items(), key=operator.itemgetter(1), reverse=True)

            d_m = []
            for item in sorted_data[:min(len(sorted_data), self.top)]:
                ratio = float(item[1])/totalamount
                d_m.append({"Name": item[0], "Amount":float(item[1]), "Percent": ratio})
                yield store_id, "{}{sep}{}{sep}A{}".format(item[0], ratio, item[1], sep=MIGO_SEPARATOR_LEVEL1)

            d_q = []
            for item in sorted_data_q[:min(len(sorted_data_q), self.top)]:
                ratio = float(item[1])/totalqty
                d_q.append({"Name": item[0], "Qty":float(item[1]), "Percent": ratio})
                yield store_id, "{}{sep}{}{sep}Q{}".format(item[0], ratio, item[1], sep=MIGO_SEPARATOR_LEVEL1)
        
            ins = {
                "ShopID": store_id.split("^")[0],
                "StoreID": store_id,
                "PeriodType": "L" + str(self.period) + "D",
                "CalDate": datetime.datetime.strptime(self.cal_date, '%Y%m%d'),
                "TopSales": {"qty":d_q,"amount":d_m}
            }

            #self.col.remove({"StoreID": store_id, "PeriodType": "L" + str(self.period) + "D", "CalDate": datetime.datetime.strptime(self.cal_date, '%Y%m%d')})
            self.add(ins)
            self.count_success += 1
        else:
            self.count_fail += 1

class CalDashBoardMember(MigoLuigiHdfs):
    """
           JobTask: CalDashBoardRevenue
        Objectives: Calculate the revenue for every stores
            Author: Wendell
      CreationDate: 2015/02/10
            Source: /user/athena/nes_member_week/20140817
                    kgsupermarket^C001  E0  55474
                    kgsupermarket^C001  EB  68
                    kgsupermarket^C001  N   929
                    kgsupermarket^C001  S1  6575
                    kgsupermarket^C001  S2  4148
                    kgsupermarket^C001  S3  97997
                    kgsupermarket^C002  S1  7459
       Destination: None
              Note: Save the results to the MongoDB
             Usage: python dashboard.py CalDashBoardMember --use-hadoop --src '/user/rungchi/testing/nes_member_week/20140817/' --cal-date 20140817
    """

    period = luigi.IntParameter(default=7)
    collection = "DashBoardMember"

    def start(self):
        super(CalDashBoardMember, self).start()

        if self.keep_temp and self.hdfsClient.exists(self.dest):
            pass
        else:
            client = MongoClient(MIGO_MONGO_TA_URL)
            db = client.ProjectReport_StarterDIY
            col = db[self.collection]

            col.remove({"ShopID":self.shop_id, "PeriodType": "L" + str(self.period) + "D", "CalDate": datetime.datetime.strptime(self.cal_date, '%Y%m%d')})

            client.close()

    def mapper(self, line):
        store_id, tag, number = line.split(MIGO_SEPARATOR_LEVEL1)
        yield store_id, (tag, int(number))

    def init_reducer(self):
        self.client = MongoClient(MIGO_MONGO_TA_URL)
        self.db = self.client.ProjectReport_StarterDIY
        self.col = self.db[self.collection]

        self.pool = []

    def add(self, record):
        self.pool.append(record)

        if len(self.pool) >= MIGO_MONGO_TA_BATCH_NUM:
            self.col.insert(self.pool)
            self.pool = []

    def end_reducer(self):
        super(CalDashBoardMember, self).end_reducer()

        if len(self.pool) > 0:
            self.col.insert(self.pool)
            self.pool = []

        self.client.close()

    def reducer(self, key, values):
        storeID = key
        v = list(values)
        s3 = 0
        total = 0
        for x in v:
            tag, members = x
            if tag == MIGO_NES_TAG_S3:
                s3 = members

            total += members

        ins = {
            "ShopID": storeID.split("^")[0],
            "StoreID": storeID,
            "PeriodType": "L" + str(self.period) + "D",
            "CalDate": datetime.datetime.strptime(self.cal_date, '%Y%m%d'),
            "TotalMember": total,
            "ValidMember": total-s3
        }

        #self.col.remove({"StoreID":storeID, "PeriodType": "L" + str(self.period) + "D", "CalDate": datetime.datetime.strptime(self.cal_date, '%Y%m%d')}) #remove old data by storeid, period, caldate
        self.add(ins)

        yield storeID, "{}{sep}{}".format(total-s3, total, sep=MIGO_SEPARATOR_LEVEL1)

        self.count_success += 1

if __name__ == "__main__": 
    luigi.run()
