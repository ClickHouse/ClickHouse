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


def test_rados_disk_simple(cluster):
    try:
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
        node.query("DROP TABLE ceph_simple_table SYNC")
    finally:
        node.query("DROP TABLE IF EXISTS ceph_simple_table")


def test_rados_disk_stripper(cluster):
    try:
        node = cluster.instances["node"]
        node.query(
            """
            CREATE TABLE ceph_big_table (
                id Int64,
                data String
            ) ENGINE=MergeTree()
            ORDER BY id
            SETTINGS
                storage_policy='ceph', min_bytes_for_wide_part=32
            """
        )

        node.query(
            "INSERT INTO ceph_big_table SELECT number, randomPrintableASCII(1024) FROM numbers(8192)"
        )
        node.query("OPTIMIZE TABLE ceph_big_table FINAL")
        assert node.query("SELECT count() FROM ceph_big_table") == "8192\n"
        assert (
            node.query("SELECT id, length(data) FROM ceph_big_table WHERE id = 1")
            == "1\t1024\n"
        )
        assert (
            node.query("SELECT id, length(data) FROM ceph_big_table WHERE id = 8191")
            == "8191\t1024\n"
        )
        node.query("DROP TABLE ceph_big_table SYNC")
        # Test that all data in pool `clickhouse` is deleted
        ceph_instance_id = cluster.get_instance_docker_id("ceph1")
        assert (
            cluster.exec_in_container(
                ceph_instance_id, ["rados", "ls", "-p", "clickhouse", "--all"]
            )
            == ""
        )
    finally:
        node.query("DROP TABLE IF EXISTS ceph_big_table")
