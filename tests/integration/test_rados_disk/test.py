#!/usr/bin/env python3

import logging
import time
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
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def get_objects_in_data_path():
    ceph_instance_id = cluster.get_instance_docker_id("ceph1")
    return cluster.exec_in_container(
        ceph_instance_id, ["rados", "--pool", "clickhouse", "--all", "ls"]
    )


def test_simple(started_cluster):
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
        node.query(
            "INSERT INTO ceph_simple_table SELECT number, toString(number) FROM numbers(2, 128)"
        )
        for i in range(2, 128):
            assert node.query(
                "SELECT * FROM ceph_simple_table WHERE id = {}".format(i)
            ) == "{}\t{}\n".format(i, i)
        node.query("DROP TABLE ceph_simple_table SYNC")
    finally:
        node.query("DROP TABLE IF EXISTS ceph_simple_table")


# Table with big parts, so single ClickHouse object may map to multiple rados objects
def test_stripper(started_cluster):
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
                storage_policy='ceph', min_bytes_for_wide_part=32, index_granularity = 16
            """
        )

        node.query(
            "INSERT INTO ceph_big_table SELECT number, randomPrintableASCII(2048) FROM numbers(8192)"
        )
        node.query("OPTIMIZE TABLE ceph_big_table FINAL")
        # Adding WHERE ignore(*) to reproduce the bug when reading with async buffer from cache
        assert (
            node.query("SELECT count() FROM ceph_big_table WHERE NOT ignore(*)")
            == "8192\n"
        )
        assert (
            node.query("SELECT id, length(data) FROM ceph_big_table WHERE id = 1")
            == "1\t2048\n"
        )
        assert (
            node.query("SELECT id, length(data) FROM ceph_big_table WHERE id = 8191")
            == "8191\t2048\n"
        )
        node.query("DROP TABLE ceph_big_table SYNC")

        # Test that all data in pool `clickhouse` is deleted
        assert get_objects_in_data_path() == ""
    finally:
        node.query("DROP TABLE IF EXISTS ceph_big_table")


# Spare column may generate empty file, they must exist in the object storage
def test_spare_column(started_cluster):
    try:
        node = cluster.instances["node"]
        node.query(
            """
            CREATE TABLE ceph_spare_table (
                id Int64,
                data String
            ) ENGINE=MergeTree()
            PARTITION BY id
            ORDER BY tuple()
            SETTINGS
                storage_policy='ceph', min_bytes_for_wide_part=32
            """
        )

        node.query("INSERT INTO ceph_spare_table SELECT 0, '' FROM numbers(8192)")
        node.query("OPTIMIZE TABLE ceph_spare_table FINAL")
        assert node.query("SELECT count() FROM ceph_spare_table") == "8192\n"
        assert (
            node.query("SELECT * FROM ceph_spare_table WHERE id = 0 LIMIT 1") == "0\t\n"
        )
    finally:
        node.query("DROP TABLE IF EXISTS ceph_spare_table")


# If some part is in detached state, it must be deleted from the object storage by DROP DETACHED PARTITION/PART
def test_drop_detached(started_cluster):
    try:
        node = cluster.instances["node"]
        node.query(
            """
            CREATE TABLE ceph_partitioned_table (
                id Int64,
                data String
            ) ENGINE=MergeTree()
            PARTITION BY id
            ORDER BY tuple()
            SETTINGS
                storage_policy='ceph', min_bytes_for_wide_part=32, old_parts_lifetime = 0
            """
        )

        objects_before = get_objects_in_data_path()

        node.query(
            "INSERT INTO ceph_partitioned_table SELECT 0, randomPrintableASCII(2048) FROM numbers(8192)"
        )
        assert node.query("SELECT count() FROM ceph_partitioned_table") == "8192\n"

        node.query("ALTER TABLE ceph_partitioned_table DETACH PARTITION 0")
        node.query(
            "ALTER TABLE ceph_partitioned_table DROP DETACHED PARTITION 0 SETTINGS allow_drop_detached = 1"
        )
        # Wait till all parts are deleted
        now = time.time()
        success = False
        while time.time() - now < 120:
            if (
                node.query(
                    "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'ceph_partitioned_table'"
                )
                == "0\n"
            ):
                success = True
                break
            time.sleep(1)

        assert success, "Old parts are not deleted"
        objects_after = get_objects_in_data_path()
        assert objects_before == objects_after

    finally:
        node.query("DROP TABLE IF EXISTS ceph_partitioned_table")


# Same test in https://github.com/ClickHouse/ClickHouse/pull/55309
def test_drop_complex_columns(started_cluster):
    try:
        start_objects = get_objects_in_data_path()
        print("Objects before", start_objects)
        node = cluster.instances["node"]
        node.query(
            """
    CREATE TABLE ceph_complex_tables(
    c1	Int8,
    c2	Date,
    `c3.k`	Array(String),
    `c3.v1`	Array(Int64),
    `c3.v3`	Array(Int64),
    `c3.v4`	Array(Int64)
    ) ENGINE = MergeTree
    order by (c1,c2) SETTINGS storage_policy = 'ceph',
    min_bytes_for_wide_part=1,
    vertical_merge_algorithm_min_rows_to_activate=1,
    vertical_merge_algorithm_min_columns_to_activate=1;"""
        )

        node.query(
            "insert into ceph_complex_tables values(1,toDate('2020-10-01'), ['a','b'], [1,2], [3,4], [5,6])"
        )

        node.query(
            "insert into ceph_complex_tables values(1,toDate('2020-10-01'), ['a','b'], [7,8], [9,10], [11,12])"
        )

        print("Objects in insert", get_objects_in_data_path())
        node.query("optimize table ceph_complex_tables final")

        print("Objects in optimize", get_objects_in_data_path())

        node.query("DROP TABLE ceph_complex_tables SYNC")
        end_objects = get_objects_in_data_path()
        print("Objects after drop", end_objects)
        assert start_objects == end_objects

    finally:
        node.query("DROP TABLE IF EXISTS ceph_complex_tables")
