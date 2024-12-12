#!/usr/bin/env python3
import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers, start_s3_mock
from helpers.utility import SafeThread, generate_values, replace_config
from helpers.wait_for_helpers import (
    wait_for_delete_empty_parts,
    wait_for_delete_inactive_parts,
    wait_for_merges,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def init_broken_s3(cluster):
    yield start_s3_mock(cluster, "broken_s3", "8083")


@pytest.fixture(scope="function")
def broken_s3(init_broken_s3):
    init_broken_s3.reset()
    yield init_broken_s3


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/storage_policy.xml",
            ],
            with_minio=True,
        )

        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_async_alter_move(cluster, broken_s3):
    node = cluster.instances["node"]

    node.query(
        """
    CREATE TABLE moving_table_async
    (
        key UInt64,
        data String
    )
    ENGINE MergeTree()
    ORDER BY tuple()
    SETTINGS storage_policy = 'slow_s3'
    """
    )

    node.query(
        "INSERT INTO moving_table_async SELECT number, randomPrintableASCII(1000) FROM numbers(10000)"
    )

    broken_s3.setup_slow_answers(
        timeout=5,
        count=1000000,
    )

    node.query(
        "ALTER TABLE moving_table_async MOVE PARTITION tuple() TO DISK 'broken_s3'",
        settings={"alter_move_to_space_execute_async": True},
        timeout=10,
    )

    # not flaky, just introduce some wait
    time.sleep(3)

    for i in range(100):
        count = node.query(
            "SELECT count() FROM system.moves where table = 'moving_table_async'"
        )
        if count == "1\n":
            break
        time.sleep(0.1)
    else:
        assert False, "Cannot find any moving background operation"


def test_sync_alter_move(cluster, broken_s3):
    node = cluster.instances["node"]

    node.query(
        """
    CREATE TABLE moving_table_sync
    (
        key UInt64,
        data String
    )
    ENGINE MergeTree()
    ORDER BY tuple()
    SETTINGS storage_policy = 'slow_s3'
    """
    )

    node.query(
        "INSERT INTO moving_table_sync SELECT number, randomPrintableASCII(1000) FROM numbers(10000)"
    )

    broken_s3.reset()

    node.query(
        "ALTER TABLE moving_table_sync MOVE PARTITION tuple() TO DISK 'broken_s3'",
        timeout=30,
    )
    # not flaky, just introduce some wait
    time.sleep(3)

    assert (
        node.query("SELECT count() FROM system.moves where table = 'moving_table_sync'")
        == "0\n"
    )

    assert (
        node.query(
            "SELECT disk_name FROM system.parts WHERE table = 'moving_table_sync'"
        )
        == "broken_s3\n"
    )
