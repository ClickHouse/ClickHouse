#!/usr/bin/env python3

import logging
import os
import time


from helpers.cluster import ClickHouseCluster
import pytest


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/storage_conf.xml",
            ],
            user_configs=[
                "configs/setting.xml",
            ],
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_paranoid_check_in_logs(cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_failover_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        """
    )

    node.query("INSERT INTO s3_failover_test VALUES (1, 'Hello')")

    assert node.contains_in_log("exists after upload")

    assert node.query("SELECT * FROM s3_failover_test ORDER BY id") == "1\tHello\n"
