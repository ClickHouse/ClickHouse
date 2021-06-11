import logging

import pytest
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node", config_dir="configs", with_minio=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def get_query_stat(instance, hint):
    result = {}
    instance.query("SYSTEM FLUSH LOGS")
    events = instance.query('''
        SELECT ProfileEvents.Names, ProfileEvents.Values
        FROM system.query_log
        ARRAY JOIN ProfileEvents
        WHERE type != 1 AND query LIKE '%{}%'
        '''.format(hint.replace("'", "\\'"))).split("\n")
    for event in events:
        ev = event.split("\t")
        if len(ev) == 2:
            if ev[0].startswith("S3"):
                result[ev[0]] = int(ev[1])
    return result


def test_write_is_cached(cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='s3'
        """
    )

    node.query("SYSTEM FLUSH LOGS")
    node.query("TRUNCATE TABLE system.query_log")

    node.query("INSERT INTO s3_test VALUES (0,'data'),(1,'data')")

    select_query = "SELECT * FROM s3_test order by id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    stat = get_query_stat(node, select_query)
    assert stat["S3ReadRequestsCount"] == 2  # Only .bin files should be accessed from S3.

    node.query("DROP TABLE IF EXISTS s3_test NO DELAY")


def test_read_after_cache_is_wiped(cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='s3'
        """
    )

    node.query("SYSTEM FLUSH LOGS")
    node.query("TRUNCATE TABLE system.query_log")

    node.query("INSERT INTO s3_test VALUES (0,'data'),(1,'data')")

    # Wipe cache
    cluster.exec_in_container(cluster.get_container_id("node"), ["rm", "-rf", "/var/lib/clickhouse/disks/s3/cache/"])

    select_query = "SELECT * FROM s3_test"
    node.query(select_query)
    stat = get_query_stat(node, select_query)
    assert stat["S3ReadRequestsCount"] == 4  # .mrk and .bin files should be accessed from S3.

    # After cache is populated again, only .bin files should be accessed from S3.
    select_query = "SELECT * FROM s3_test order by id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"
    stat = get_query_stat(node, select_query)
    assert stat["S3ReadRequestsCount"] == 2

    node.query("DROP TABLE IF EXISTS s3_test NO DELAY")
