import logging

import pytest
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node", main_configs=["configs/config.d/storage_conf.xml", "configs/config.d/ssl_conf.xml",
                                                   "configs/config.d/query_log.xml"],
                             user_configs=["configs/config.d/users.xml"], with_minio=True)
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
        SELECT ProfileEvents.keys, ProfileEvents.values
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


@pytest.mark.parametrize("min_rows_for_wide_part,read_requests", [(0, 2), (8192, 1)])
def test_write_is_cached(cluster, min_rows_for_wide_part, read_requests):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='s3', min_rows_for_wide_part={}
        """.format(min_rows_for_wide_part)
    )

    node.query("SYSTEM FLUSH LOGS")
    node.query("TRUNCATE TABLE system.query_log")

    node.query("INSERT INTO s3_test VALUES (0,'data'),(1,'data')")

    select_query = "SELECT * FROM s3_test order by id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    stat = get_query_stat(node, select_query)
    assert stat["S3ReadRequestsCount"] == read_requests  # Only .bin files should be accessed from S3.

    node.query("DROP TABLE IF EXISTS s3_test NO DELAY")

@pytest.mark.parametrize("min_rows_for_wide_part,all_files,bin_files", [(0, 4, 2), (8192, 2, 1)])
def test_read_after_cache_is_wiped(cluster, min_rows_for_wide_part, all_files, bin_files):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='s3', min_rows_for_wide_part={}
        """.format(min_rows_for_wide_part)
    )

    node.query("SYSTEM FLUSH LOGS")
    node.query("TRUNCATE TABLE system.query_log")

    node.query("INSERT INTO s3_test VALUES (0,'data'),(1,'data')")

    # Wipe cache
    cluster.exec_in_container(cluster.get_container_id("node"), ["rm", "-rf", "/var/lib/clickhouse/disks/s3/cache/"])

    select_query = "SELECT * FROM s3_test"
    node.query(select_query)
    stat = get_query_stat(node, select_query)
    assert stat["S3ReadRequestsCount"] == all_files  # .mrk and .bin files should be accessed from S3.

    # After cache is populated again, only .bin files should be accessed from S3.
    select_query = "SELECT * FROM s3_test order by id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"
    stat = get_query_stat(node, select_query)
    assert stat["S3ReadRequestsCount"] == bin_files

    node.query("DROP TABLE IF EXISTS s3_test NO DELAY")
