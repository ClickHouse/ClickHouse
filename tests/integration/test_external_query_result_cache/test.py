## sudo -H pip install redis
import json
import struct
import sys

import pytest
import redis
import uuid 
import time 

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1",  main_configs = ["configs/query_result_cache.xml"], with_redis=True, randomize_settings=False)
node2 = cluster.add_instance("node2",  main_configs = ["configs/query_result_cache.xml"], with_redis=True, randomize_settings=False)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_redis_connection(db_id=0):
    client = redis.Redis(
        host="172.16.1.2", port=6379, password="clickhouse", db=db_id
    )
    return client

def check_cache_hits(node, query_id):
    query = "select ProfileEvents['QueryCacheHits'] from system.query_log where query_id='{}' and type='QueryFinish'".format(query_id)
    result = node.query(query)  
    return result.strip() == '1'

def check_cache_misses(node, query_id):
    query = "select ProfileEvents['QueryCacheMisses'] from system.query_log where query_id='{}' and type='QueryFinish'".format(query_id)
    result = node.query(query)  
    return result.strip() == '1'

def test_simple_select(started_cluster):
    client = get_redis_connection()

    # clean all
    client.flushall()


    settings = {"use_query_cache":1, "query_cache_ttl":10}
    query_id_node1_1 = str(uuid.uuid4())
    query_id_node1_2 = str(uuid.uuid4())
    query_id_node2_1 = str(uuid.uuid4())
    
    #The first time the query is executed, the cache does not hit.
    node1.query(f"select 1", settings = settings, query_id=query_id_node1_1)
    node1.query(f"select 1", settings = settings, query_id=query_id_node1_2)
    node2.query(f"select 1", settings = settings, query_id=query_id_node2_1)
    node1.query(f"system flush logs")
    node2.query(f"system flush logs")

    assert (check_cache_misses(node1, query_id_node1_1) == True)
    assert (check_cache_hits(node1, query_id_node1_2) == True)
    assert (check_cache_hits(node2, query_id_node2_1) == True)

def test_clear_external_cache(started_cluster):
    client = get_redis_connection()

    # clean all
    client.flushall()


    settings = {"use_query_cache":1, "query_cache_ttl":10}
    query_id_node1_1 = str(uuid.uuid4())
    query_id_node1_2 = str(uuid.uuid4())
    query_id_node2_1 = str(uuid.uuid4())
    
    #The first time the query is executed, the cache does not hit.
    node1.query(f"select 1", settings = settings, query_id=query_id_node1_1)
   
    # clean all
    client.flushall()
    node1.query(f"select 1", settings = settings, query_id=query_id_node1_2)
    node2.query(f"select 1", settings = settings, query_id=query_id_node2_1)
    
    node1.query(f"system flush logs")
    node2.query(f"system flush logs")
    
    assert (check_cache_misses(node1, query_id_node1_1) == True)
    assert (check_cache_misses(node1, query_id_node1_2) == True)
    assert (check_cache_hits(node2, query_id_node2_1) == True)

def test_external_cache_ttl(started_cluster):
    client = get_redis_connection()

    # clean all
    client.flushall()


    settings = {"use_query_cache":1, "query_cache_ttl":2}
    query_id_node1_1 = str(uuid.uuid4())
    query_id_node1_2 = str(uuid.uuid4())
    query_id_node2_1 = str(uuid.uuid4())
    
    #The first time the query is executed, the cache does not hit.
    node1.query(f"select 1", settings = settings, query_id=query_id_node1_1)
    node1.query(f"select 1", settings = settings, query_id=query_id_node1_2)
    
    time.sleep(2)
    node2.query(f"select 1", settings = settings, query_id=query_id_node2_1)
    
    node1.query(f"system flush logs")
    node2.query(f"system flush logs")
    

    assert (check_cache_misses(node1, query_id_node1_1) == True)
    assert (check_cache_hits(node1, query_id_node1_2) == True)
    assert (check_cache_misses(node2, query_id_node2_1) == True)
