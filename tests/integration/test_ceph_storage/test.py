#!/usr/bin/env python3

import logging
import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/storage_conf.xml",
            ],
            with_ceph=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()

def test_simple_ceph_disk(cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE ceph_simple_table (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS
            storage_policy='ceph', min_bytes_for_wide_part=32
        """
    )

    node.query("INSERT INTO ceph_simple_table VALUES (1, 'hello')")
    assert node.query("SELECT * FROM ceph_simple_table") == "1\thello\n"
    node.query("INSERT INTO ceph_simple_table SELECT number, toString(number) FROM numbers(2, 128)")
    for i in range(2, 128):
        assert node.query("SELECT * FROM ceph_simple_table WHERE id = {}".format(i)) == "{}\t{}\n".format(i, i)