# -*- coding:utf-8 -*-
import dbOpt
import sys
from dbConf import dbinfo
params=sys.argv

connFrom=dbOpt.MysqlDao(dbinfo.get("%s"%params[2]))
connTo=dbOpt.MysqlDao(dbinfo.get("%s"%params[3]))
nums=connFrom.select("desc %s"%params[1])
mystr,j='',0
while j<len(nums):
    mystr+=r',%s'
    j+=1
insertSQL='insert into %s values(%s)'%(params[1],mystr[1:])
i=0
print params[1]
connTo.truncate(params[1])
counts=0  # 用于计数
rows=5000 # 每次提交条数
while True:
    sql="select * from %s limit %s,%s" %(params[1],i,rows)
    data=connFrom.select(sql)
    counts+=len(data)
    if len(data)==0:
        break
    else:
        connTo.bachInsert(insertSQL,data)         
    i+=rows
    print i
connFrom.release()
connTo.release()
print 'over,counts:%s'%counts
