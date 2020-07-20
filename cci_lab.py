from multiprocessing import Pool, Manager
import time
import argparse
import json
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from boto3.dynamodb.conditions import Key
import random

parser=argparse.ArgumentParser()

parser.add_argument('--table', type=str, help='Name of the table to load')
parser.add_argument('--region', type=str, help='Region of the table')
parser.add_argument('--workers', type=int, help='Number of workers to spawn')
parser.add_argument('--pk-val', type=str, help='PK val to load with queries')

args=parser.parse_args()

table = args.table
region = args.region
workers = int(args.workers)
pk_val = args.pk_val

manager = Manager()
consumedCapacities = manager.list()

def writer(pk_val):
    dynamodb = boto3.resource('dynamodb', region_name=region)
    dynoTable = dynamodb.Table(table)
    num_writes = random.randint(10,299)
    for i in range(num_writes):
        try:
            response = dynoTable.put_item(
                Item={'id': '000' + str(pk_val),
                'age': str(random.randint(0,999999))}, 
                ReturnConsumedCapacity='TOTAL'
            )
            print(response)
            consumedCapacities.append(float(response["ConsumedCapacity"]["CapacityUnits"]))
        except ClientError as e:
            print(e)

def reader(pk_val):
    dynamodb = boto3.resource('dynamodb', region_name=region)
    dynoTable = dynamodb.Table(table)
    for i in range(100):
        try:
          response = dynoTable.query(
              KeyConditionExpression=Key('id').eq(pk_val),
              ReturnConsumedCapacity='TOTAL'
          )
          print(response)
          consumedCapacities.append(float(response["ConsumedCapacity"]["CapacityUnits"]))
        except ClientError as e:
            print(e)



if __name__ == '__main__':

    start = time.time()
    som = []
    for i in range(100):
        if not pk_val:
          som.append(random.randint(0, 9999))
        else: 
          som.append(pk_val)
    
    pool = Pool(workers)
    if pk_val:
      pool.map(reader, som)
    else:
      pool.map(writer, som)
    totalConsumedCapacity = 0
    for capacity in consumedCapacities:
        totalConsumedCapacity += capacity

    end = time.time()  
    total_time = round(end-start,2)    
    print('==============================================================')
    if pk_val:
      print(f'Consumed {totalConsumedCapacity} RCUs in {total_time} seconds (avg. {round(totalConsumedCapacity/total_time,2)} RCU/s)')
    else:
      print(f'Consumed {totalConsumedCapacity} WCUs in {total_time} seconds (avg. {round(totalConsumedCapacity/total_time,2)} WCU/s)')
    print('==============================================================')
    
