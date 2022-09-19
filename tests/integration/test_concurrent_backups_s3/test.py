#!/usr/bin/env python3
import pytest
import re
import os.path
from multiprocessing.dummy import Pool
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
import time

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_concurrent_backups(start_cluster):
    node.query("DROP TABLE IF EXISTS s3_test NO DELAY")
    columns = [f"column_{i} UInt64" for i in range(1000)]
    columns_str = ", ".join(columns)
    node.query(
        f"CREATE TABLE s3_test ({columns_str}) Engine=MergeTree() ORDER BY tuple() SETTINGS storage_policy='s3';"
    )
    node.query(
        f"INSERT INTO s3_test SELECT * FROM generateRandom('{columns_str}') LIMIT 10000"
    )

    def create_backup(i):
        backup_name = f"Disk('hdd', '/backups/{i}')"
        node.query(f"BACKUP TABLE s3_test TO {backup_name} ASYNC")

    p = Pool(40)

    p.map(create_backup, range(40))

    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.backups WHERE status != 'BACKUP_COMPLETE' and status != 'FAILED_TO_BACKUP'",
        "0",
        retry_count=100,
    )
    assert node.query("SELECT count() FROM s3_test where not ignore(*)") == "10000\n"
