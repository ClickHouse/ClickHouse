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

def check_cache_hits(node, query_id):
    query = "select ProfileEvents['QueryCacheHits'] from system.query_log where query_id='{}' and type='QueryFinish'".format(query_id)
    result = node.query(query)
    return result.strip() == '1'

def check_cache_misses(node, query_id):
    query = "select ProfileEvents['QueryCacheMisses'] from system.query_log where query_id='{}' and type='QueryFinish'".format(query_id)
    result = node.query(query)
    return result.strip() == '1'

def drop_query_cache():
    node1.query(f"system drop query cache")

def test_simple_select(started_cluster):
    drop_query_cache()

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

    drop_query_cache()

def test_clear_external_cache(started_cluster):

    drop_query_cache()
    settings = {"use_query_cache":1, "query_cache_ttl":10}
    query_id_node1_1 = str(uuid.uuid4())
    query_id_node1_2 = str(uuid.uuid4())
    query_id_node2_1 = str(uuid.uuid4())

    #The first time the query is executed, the cache does not hit.
    node1.query(f"select 1", settings = settings, query_id=query_id_node1_1)

    # clean all
    drop_query_cache()

    node1.query(f"select 1", settings = settings, query_id=query_id_node1_2)
    node2.query(f"select 1", settings = settings, query_id=query_id_node2_1)

    node1.query(f"system flush logs")
    node2.query(f"system flush logs")

    assert (check_cache_misses(node1, query_id_node1_1) == True)
    assert (check_cache_misses(node1, query_id_node1_2) == True)
    assert (check_cache_hits(node2, query_id_node2_1) == True)
    drop_query_cache()

def test_external_cache_ttl(started_cluster):
    drop_query_cache()

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
    drop_query_cache()
