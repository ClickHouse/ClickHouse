import os
import logging
import random
import string
import time
import threading
from collections import Counter

import pytest
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance("node1", main_configs=["configs/config.d/storage_conf.xml"],
                            macros={'replica': '1'},
                            with_nfs=True,
                            with_zookeeper=True)
        cluster.add_instance("node2", main_configs=["configs/config.d/storage_conf.xml"],
                            macros={'replica': '2'},
                            with_nfs=True,
                            with_zookeeper=True)

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


FILES_OVERHEAD = 1
FILES_OVERHEAD_PER_COLUMN = 2  # Data and mark files
FILES_OVERHEAD_PER_PART_WIDE = FILES_OVERHEAD_PER_COLUMN * 3 + 2 + 6 + 1
FILES_OVERHEAD_PER_PART_COMPACT = 10 + 1


def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def create_table(cluster, create_table_statement, additional_settings=None):
    if additional_settings:
        create_table_statement += ","
        create_table_statement += additional_settings
    list(cluster.instances.values())[0].query(create_table_statement)

def debug_cluster(cluster, sql):
    for name, node in cluster.instances.items():
        result = node.query(sql)
        logging.debug("%s:%s, result :\n%s" % (name, sql, result))

def assert_cluster(cluster, sql , value, debug=False):
    for name, node in cluster.instances.items():
        result = node.query(sql)
        if debug:
            logging.debug("%s:%s, result : %s" % (name, sql, result))
        assert result == value

def query_cluster(cluster, sql,debug=False):
    for name, node in cluster.instances.items():
        if debug:
            logging.debug(name + ":" + node.query(sql))
        node.query(sql)

def wait_for_query(cluster, sql , value, debug=False):
    for i in range(3): # wait for replication 3 seconds max
        def query_result(name, node, query):
            val = node.query(query).rstrip()
            if debug:
                logging.debug("%s, %s, result:%s" % (name, query, val))
            return val
        if all([query_result(name, node, sql) == value for name, node in cluster.instances.items()]):
            return True
        time.sleep(1)
    return False


def wait_for_mutations(nodes, table, number_of_mutations):
    for i in range(100):  # wait for replication 80 seconds max
        time.sleep(0.8)

        def get_done_mutations(node):
            return int(node.query("SELECT sum(is_done) FROM system.mutations WHERE table = '%s'" % table).rstrip())

        if all([get_done_mutations(n) == number_of_mutations for n in nodes]):
            return True
    return False


def test_nfs_zero_copy_replication_insert(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    nodes = list(cluster.instances.values())

    try:
        node1.query(
            """
            CREATE TABLE nfs_test_insert ON CLUSTER nfs_cluster (dt DateTime, id Int64)
            ENGINE=ReplicatedMergeTree('/clickhouse/{cluster}/nfs_test_insert/{shard}/', '{replica}')
            ORDER BY (dt, id)
            SETTINGS storage_policy='p_nfs'
            """
        )
        node1.query("INSERT INTO nfs_test_insert VALUES (now() - INTERVAL 3 DAY, 10)")
        node2.query("SYSTEM SYNC REPLICA nfs_test_insert")
        assert_cluster(cluster, "SELECT count() FROM nfs_test_insert FORMAT Values", "(1)", True)
        assert_cluster(cluster, "SELECT id FROM nfs_test_insert ORDER BY dt FORMAT Values", "(10)", True)
        assert_cluster(cluster, "SELECT partition_id,disk_name FROM system.parts WHERE table='nfs_test_insert' FORMAT Values", "('all','d_nfs')", True)

    finally:
        query_cluster(cluster, "DROP TABLE IF EXISTS nfs_test_insert NO DELAY")


def test_nfs_zero_copy_replication_insert_nomerge(cluster):
    try:
        create_table_statement = """
            CREATE TABLE nfs_test_nomerge ON CLUSTER nfs_cluster(
                dt Date,
                id Int64,
                data String,
                INDEX min_max (id) TYPE minmax GRANULARITY 3
            )
            ENGINE=ReplicatedMergeTree('/clickhouse/{cluster}/nfs_test_nomerge/{shard}', '{replica}')
            PARTITION BY dt
            ORDER BY (dt, id)
            SETTINGS storage_policy='p_nfs'
            """
        create_table(cluster, create_table_statement, additional_settings="min_rows_for_wide_part=0")

        query_cluster(cluster, "SYSTEM STOP MERGES nfs_test_nomerge")

        all_values = ""
        for node_idx in range(1, 2):
            node = cluster.instances["node" + str(node_idx)]
            values = generate_values("2020-01-0" + str(node_idx), 1024)
            node.query("INSERT INTO nfs_test_nomerge VALUES {}".format(values), settings={"insert_quorum": 1})
            if node_idx != 1:
                all_values += ","
            all_values += values

        for node_idx in range(1, 2):
            node = cluster.instances["node" + str(node_idx)]
            assert node.query("SELECT * FROM nfs_test_nomerge order by dt, id FORMAT Values",
                            settings={"select_sequential_consistency": 1}) == all_values
    finally:
        query_cluster(cluster, "DROP TABLE IF EXISTS nfs_test_nomerge NO DELAY")


def test_nfs_zero_copy_with_ttl_delete(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    try:
        create_table_statement = """
            CREATE TABLE ttl_delete_test ON CLUSTER nfs_cluster (dt DateTime, id Int64)
            ENGINE=ReplicatedMergeTree('/clickhouse/{cluster}/ttl_delete_test/{shard}', '{replica}')
            ORDER BY (dt, id)
            TTL dt + INTERVAL 2 DAY
            SETTINGS storage_policy='p_nfs'
            """
        create_table(cluster, create_table_statement, additional_settings="number_of_free_entries_in_pool_to_execute_mutation=0")

        node1.query("INSERT INTO ttl_delete_test VALUES (now() - INTERVAL 3 DAY, 10)")
        node1.query("OPTIMIZE TABLE ttl_delete_test FINAL")
        node2.query("SYSTEM SYNC REPLICA ttl_delete_test")
        wait_for_query(cluster, "SELECT count() FROM ttl_delete_test FORMAT Values", "(0)", True)

        node1.query("INSERT INTO ttl_delete_test VALUES (now() - INTERVAL 1 DAY, 11)")
        node1.query("OPTIMIZE TABLE ttl_delete_test FINAL")
        node2.query("SYSTEM SYNC REPLICA ttl_delete_test")
        wait_for_query(cluster, "SELECT count() FROM ttl_delete_test FORMAT Values", "(1)", True)
        wait_for_query(cluster, "SELECT id FROM ttl_delete_test ORDER BY id FORMAT Values", "(11)", True)

    finally:
        query_cluster(cluster, "DROP TABLE IF EXISTS ttl_delete_test NO DELAY")


def test_nfs_zero_copy_mutations(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    nodes = list(cluster.instances.values())

    try:
        create_table_statement = """
            CREATE TABLE nfs_test_mutations ON CLUSTER nfs_cluster (d Date, x UInt32, i UInt32)
            ENGINE ReplicatedMergeTree('/clickhouse/{cluster}/nfs_test_mutations/{shard}', '{replica}')
            ORDER BY x
            PARTITION BY toYYYYMM(d)
            SETTINGS storage_policy='p_nfs'
            """
        create_table(cluster, create_table_statement, additional_settings="number_of_free_entries_in_pool_to_execute_mutation=0")

        for year in range(2000, 2016):
            rows = ''
            date_str = '{}-01-{}'.format(year, random.randint(1, 10))
            for i in range(10):
                rows += '{}	{}	{}\n'.format(date_str, random.randint(1, 10), i)
            node1.query("INSERT INTO nfs_test_mutations FORMAT TSV", rows)

        node1.query("ALTER TABLE nfs_test_mutations UPDATE i = sleepEachRow(2) WHERE 1")

        all_done = wait_for_mutations([node1], "nfs_test_mutations", 1)

        for node in nodes:
            logging.debug("nfs dir : %s" % cluster.nfs_dir)
            logging.debug(os.listdir(cluster.nfs_dir))
            logging.debug(node.query("select * from system.storage_policies"))
            logging.debug(node.query("SELECT * FROM system.disks"))
            logging.debug(node.query("SELECT database, table, partition, name, path FROM system.parts where table='nfs_test_mutations'"))
            logging.debug(node.query(
                "SELECT mutation_id, command, parts_to_do, is_done FROM system.mutations WHERE table = 'nfs_test_mutations' FORMAT TSVWithNames"))
            logging.debug(node.query(
                "SELECT partition, count(name), sum(active), sum(active*rows) FROM system.parts WHERE table ='nfs_test_mutations' GROUP BY partition FORMAT TSVWithNames"))

        assert all_done == True
    finally:
        query_cluster(cluster, "DROP TABLE IF EXISTS nfs_test_mutations NO DELAY")
