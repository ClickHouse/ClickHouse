#!/usr/bin/env python3

import logging
import pytest
import random
import string

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/storage_conf.xml",
            ],
            with_ceph=True,
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


insert_values = [
    "(0,'data'),(1,'data')",
    ",".join(
        f"({i},'{''.join(random.choices(string.ascii_lowercase, k=5))}')"
        for i in range(10)
    ),
]


def get_objects_in_data_path():
    ceph_instance_id = cluster.get_instance_docker_id("ceph1")
    return cluster.exec_in_container(
        ceph_instance_id, ["rados", "--pool", "clickhouse", "--all", "ls"]
    )


def test_insert_select(started_cluster):
    node = cluster.instances["node"]

    for index, value in enumerate(insert_values):
        node.query(
            """
            CREATE TABLE test_{} (
                id Int64,
                data String
            ) ENGINE=MergeTree()
            ORDER BY id
            SETTINGS storage_policy='ceph'
            """.format(
                index
            ),
        )

        node.query("INSERT INTO test_{} VALUES {}".format(index, value))
        assert (
            node.query("SELECT * FROM test_{} ORDER BY id FORMAT Values".format(index))
            == value
        )


def test_restart_server(started_cluster):
    node = cluster.instances["node"]

    for index, value in enumerate(insert_values):
        assert (
            node.query("SELECT * FROM test_{} ORDER BY id FORMAT Values".format(index))
            == value
        )
    node.restart_clickhouse()

    for index, value in enumerate(insert_values):
        assert (
            node.query("SELECT * FROM test_{} ORDER BY id FORMAT Values".format(index))
            == value
        )


def test_drop_table(started_cluster):
    node = cluster.instances["node"]

    for index, value in enumerate(insert_values):
        node.query("DROP TABLE IF EXISTS test_{} SYNC".format(index))

    assert get_objects_in_data_path() == ""
