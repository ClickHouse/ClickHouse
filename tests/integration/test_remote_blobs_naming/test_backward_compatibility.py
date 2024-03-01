#!/usr/bin/env python3

import logging
import pytest

import os
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    cluster = ClickHouseCluster(__file__)
    cluster.add_instance(
        "node",
        main_configs=[
            "configs/old_node.xml",
            "configs/storage_conf.xml",
        ],
        user_configs=[
            "configs/settings.xml",
        ],
        with_minio=True,
        macros={"replica": "1"},
        with_zookeeper=True,
    )
    cluster.add_instance(
        "new_node",
        main_configs=[
            "configs/new_node.xml",
            "configs/storage_conf.xml",
        ],
        user_configs=[
            "configs/settings.xml",
        ],
        with_minio=True,
        macros={"replica": "2"},
        with_zookeeper=True,
    )
    cluster.add_instance(
        "switching_node",
        main_configs=[
            "configs/switching_node.xml",
            "configs/storage_conf.xml",
        ],
        user_configs=[
            "configs/settings.xml",
        ],
        with_minio=True,
        with_zookeeper=True,
        stay_alive=True,
    )
    logging.info("Starting cluster...")
    cluster.start()
    logging.info("Cluster started")

    yield cluster

    # Actually, try/finally section is excess in pytest.fixtures
    cluster.shutdown()


def get_part_path(node, table, part_name):
    part_path = node.query(
        f"SELECT path FROM system.parts WHERE table = '{table}' and name = '{part_name}'"
    ).strip()

    return os.path.normpath(part_path)


def get_first_part_name(node, table):
    part_name = node.query(
        f"SELECT name FROM system.parts WHERE table = '{table}' and active LIMIT 1"
    ).strip()
    return part_name


def read_file(node, file_path):
    return node.exec_in_container(["bash", "-c", f"cat {file_path}"])


def write_file(node, file_path, data):
    node.exec_in_container(["bash", "-c", f"echo '{data}' > {file_path}"])


def find_keys_for_local_path(node, local_path):
    remote = node.query(
        f"""
            SELECT
                remote_path
            FROM
                system.remote_data_paths
            WHERE
                concat(path, local_path) = '{local_path}'
            """
    ).split("\n")
    return [x for x in remote if x]


def test_read_new_format(cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE test_read_new_format (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        """
    )

    node.query("INSERT INTO test_read_new_format VALUES (1, 'Hello')")

    part_name = get_first_part_name(node, "test_read_new_format")
    part_path = get_part_path(node, "test_read_new_format", part_name)
    primary_idx = os.path.join(part_path, "primary.cidx")

    remote = find_keys_for_local_path(node, primary_idx)
    assert len(remote) == 1
    remote = remote[0]

    node.query(f"ALTER TABLE test_read_new_format DETACH PART '{part_name}'")

    detached_primary_idx = os.path.join(
        os.path.dirname(part_path), "detached", part_name, "primary.cidx"
    )

    # manually change the metadata format and see that CH reads it correctly
    meta_data = read_file(node, detached_primary_idx)
    lines = meta_data.split("\n")
    object_size, object_key = lines[2].split("\t")
    assert remote.endswith(object_key), object_key
    assert remote != object_key
    lines[2] = f"{object_size}\t{remote}"
    lines[0] = "5"

    write_file(node, detached_primary_idx, "\n".join(lines))

    active_count = node.query(
        f"SELECT count() FROM system.parts WHERE table = 'test_read_new_format' and active"
    ).strip()
    assert active_count == "0", active_count

    node.query(f"ALTER TABLE test_read_new_format ATTACH PART '{part_name}'")

    active_count = node.query(
        f"SELECT count() FROM system.parts WHERE table = 'test_read_new_format' and active"
    ).strip()
    assert active_count == "1", active_count

    values = node.query(f"SELECT * FROM test_read_new_format").split("\n")
    values = [x for x in values if x]
    assert values == ["1\tHello"], values

    # part name has changed after attach
    part_name = get_first_part_name(node, "test_read_new_format")
    part_path = get_part_path(node, "test_read_new_format", part_name)
    primary_idx = os.path.join(part_path, "primary.cidx")

    new_remote = find_keys_for_local_path(node, primary_idx)
    assert len(new_remote) == 1
    new_remote = new_remote[0]
    assert remote == new_remote


def test_write_new_format(cluster):
    node = cluster.instances["new_node"]

    node.query(
        """
        CREATE TABLE test_read_new_format (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        """
    )

    node.query("INSERT INTO test_read_new_format VALUES (1, 'Hello')")

    part_name = get_first_part_name(node, "test_read_new_format")
    part_path = get_part_path(node, "test_read_new_format", part_name)
    primary_idx = os.path.join(part_path, "primary.cidx")

    remote = find_keys_for_local_path(node, primary_idx)
    assert len(remote) == 1
    remote = remote[0]

    node.query(f"ALTER TABLE test_read_new_format DETACH PART '{part_name}'")

    detached_primary_idx = os.path.join(
        os.path.dirname(part_path), "detached", part_name, "primary.cidx"
    )

    # manually change the metadata format and see that CH reads it correctly
    meta_data = read_file(node, detached_primary_idx)
    lines = meta_data.split("\n")
    object_size, object_key = lines[2].split("\t")
    assert remote.endswith(object_key), object_key
    assert remote == object_key


@pytest.mark.parametrize("storage_policy", ["s3", "s3_plain"])
def test_replicated_merge_tree(cluster, storage_policy):
    if storage_policy == "s3_plain":
        # MergeTree table doesn't work on s3_plain. Rename operation is not implemented
        return

    node_old = cluster.instances["node"]
    node_new = cluster.instances["new_node"]

    create_table_statement = f"""
        CREATE TABLE test_replicated_merge_tree (
            id Int64,
            val String
        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_replicated_merge_tree_{storage_policy}', '{{replica}}')
        PARTITION BY id
        ORDER BY (id, val)
        SETTINGS
            storage_policy='{storage_policy}'
        """

    node_old.query(create_table_statement)
    node_new.query(create_table_statement)

    node_old.query("INSERT INTO test_replicated_merge_tree VALUES (0, 'a')")
    node_new.query("INSERT INTO test_replicated_merge_tree VALUES (1, 'b')")

    # node_old have to fetch metadata from node_new and vice versa
    node_old.query("SYSTEM SYNC REPLICA test_replicated_merge_tree")
    node_new.query("SYSTEM SYNC REPLICA test_replicated_merge_tree")

    count_old = node_old.query("SELECT count() FROM test_replicated_merge_tree").strip()
    count_new = node_new.query("SELECT count() FROM test_replicated_merge_tree").strip()

    assert count_old == "2"
    assert count_new == "2"

    node_old.query("DROP TABLE test_replicated_merge_tree SYNC")
    node_new.query("DROP TABLE test_replicated_merge_tree SYNC")


def switch_config_write_full_object_key(node, enable):
    setting_path = "/etc/clickhouse-server/config.d/switching_node.xml"

    is_on = "<storage_metadata_write_full_object_key>1<"
    is_off = "<storage_metadata_write_full_object_key>0<"

    data = read_file(node, setting_path)

    assert data != ""
    assert is_on in data or is_off in data

    if enable:
        node.replace_in_config(setting_path, is_off, is_on)
    else:
        node.replace_in_config(setting_path, is_on, is_off)

    node.restart_clickhouse()


@pytest.mark.parametrize("storage_policy", ["s3", "s3_plain"])
def test_log_table(cluster, storage_policy):
    if storage_policy == "s3_plain":
        # Log table doesn't work on s3_plain. Rename operation is not implemented
        return

    node = cluster.instances["switching_node"]

    create_table_statement = f"""
        CREATE TABLE test_log_table (
            id Int64,
            val String
        ) ENGINE=Log
        SETTINGS
            storage_policy='{storage_policy}'
        """

    node.query(create_table_statement)

    node.query("INSERT INTO test_log_table VALUES (0, 'a')")
    assert "1" == node.query("SELECT count() FROM test_log_table").strip()

    switch_config_write_full_object_key(node, True)
    node.query("INSERT INTO test_log_table VALUES (0, 'a')")
    assert "2" == node.query("SELECT count() FROM test_log_table").strip()

    switch_config_write_full_object_key(node, False)
    node.query("INSERT INTO test_log_table VALUES (1, 'b')")
    assert "3" == node.query("SELECT count() FROM test_log_table").strip()

    switch_config_write_full_object_key(node, True)
    node.query("INSERT INTO test_log_table VALUES (2, 'c')")
    assert "4" == node.query("SELECT count() FROM test_log_table").strip()

    node.query("DROP TABLE test_log_table SYNC")
