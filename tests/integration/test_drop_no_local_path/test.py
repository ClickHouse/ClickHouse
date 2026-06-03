#!/usr/bin/env python3

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", main_configs=["configs/remote_servers.xml"])


@pytest.fixture(scope="module")
def setup_nodes():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def drop_table_directory(table_name):
    data_path = instance.query(
        f"SELECT data_paths[1] FROM system.tables where name = '{table_name}'"
    ).strip()
    print("Data path", data_path)
    instance.exec_in_container(
        ["bash", "-c", f"rm -fr {data_path}"], privileged=True, user="root"
    )


def test_drop_no_local_path(setup_nodes):
    instance.query(
        "CREATE TABLE merge_tree_table (key UInt64) ENGINE = MergeTree() ORDER BY tuple()"
    )
    instance.query("INSERT INTO merge_tree_table VALUES (1)")
    drop_table_directory("merge_tree_table")
    instance.query("DROP TABLE merge_tree_table SYNC", timeout=10)

    instance.query(
        "CREATE TABLE merge_tree_table (key UInt64) ENGINE = MergeTree() ORDER BY tuple()"
    )

    instance.query(
        "CREATE TABLE distributed_table (key UInt64) ENGINE = Distributed(test_cluster, default, merge_tree_table, key)"
    )
    instance.query("INSERT INTO distributed_table VALUES(0)")
    drop_table_directory("distributed_table")
    instance.query("DROP TABLE distributed_table SYNC", timeout=10)

    instance.query("DROP TABLE merge_tree_table SYNC", timeout=10)

    instance.query(
        "CREATE TABLE join_table(`id` UInt64, `val` String) ENGINE = Join(ANY, LEFT, id)"
    )
    instance.query("INSERT INTO join_table VALUES (1, 'a')")

    drop_table_directory("join_table")

    instance.query("TRUNCATE TABLE join_table", timeout=10)
