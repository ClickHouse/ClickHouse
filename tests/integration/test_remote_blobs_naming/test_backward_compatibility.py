#!/usr/bin/env python3
import logging
import os
import re
from contextlib import contextmanager
from difflib import unified_diff

import pytest

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
            "configs/storage_conf_new.xml",
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


@contextmanager
def drop_table_scope(nodes, tables, create_statements):
    try:
        for node in nodes:
            for statement in create_statements:
                node.query(statement)
        yield
    finally:
        for node in nodes:
            for table in tables:
                node.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.mark.parametrize(
    "test_case",
    [
        ("s3_plain", False),
        ("s3", False),
        ("s3", True),
        ("s3_template_key", False),
        ("s3_template_key", True),
    ],
)
def test_replicated_merge_tree(cluster, test_case):
    storage_policy, zero_copy = test_case

    if storage_policy == "s3_plain":
        # MergeTree table doesn't work on s3_plain. Rename operation is not implemented
        return

    node_old = cluster.instances["node"]
    node_new = cluster.instances["new_node"]

    zk_table_path = f"/clickhouse/tables/test_replicated_merge_tree_{storage_policy}{'_zero_copy' if zero_copy else ''}"
    create_table_statement = f"""
                CREATE TABLE test_replicated_merge_tree (
                    id Int64,
                    val String
                ) ENGINE=ReplicatedMergeTree('{zk_table_path}', '{{replica}}')
                PARTITION BY id
                ORDER BY (id, val)
                SETTINGS
                    storage_policy='{storage_policy}',
                    allow_remote_fs_zero_copy_replication='{1 if zero_copy else 0}'
                """

    with drop_table_scope(
        [node_old, node_new], ["test_replicated_merge_tree"], [create_table_statement]
    ):
        node_old.query("INSERT INTO test_replicated_merge_tree VALUES (0, 'a')")
        node_new.query("INSERT INTO test_replicated_merge_tree VALUES (1, 'b')")

        # node_old have to fetch metadata from node_new and vice versa
        node_old.query("SYSTEM SYNC REPLICA test_replicated_merge_tree")
        node_new.query("SYSTEM SYNC REPLICA test_replicated_merge_tree")

        count_old = node_old.query(
            "SELECT count() FROM test_replicated_merge_tree"
        ).strip()
        count_new = node_new.query(
            "SELECT count() FROM test_replicated_merge_tree"
        ).strip()

        assert count_old == "2"
        assert count_new == "2"

        if not zero_copy:
            return

        def get_remote_pathes(node, table_name, only_remote_path=True):
            uuid = node.query(
                f"""
                SELECT uuid
                FROM system.tables
                WHERE name = '{table_name}'
                """
            ).strip()
            assert uuid
            return node.query(
                f"""
                SELECT {"remote_path" if only_remote_path else "*"}
                FROM system.remote_data_paths
                WHERE
                    local_path LIKE '%{uuid}%'
                    AND local_path NOT LIKE '%format_version.txt%'
                ORDER BY ALL
                """
            ).strip()

        remote_pathes_old = get_remote_pathes(node_old, "test_replicated_merge_tree")
        remote_pathes_new = get_remote_pathes(node_new, "test_replicated_merge_tree")

        assert len(remote_pathes_old) > 0
        assert remote_pathes_old == remote_pathes_new, (
            str(unified_diff(remote_pathes_old, remote_pathes_new))
            + "\n\nold:\n"
            + get_remote_pathes(node_old, "test_replicated_merge_tree", False)
            + "\n\nnew:\n"
            + get_remote_pathes(node_new, "test_replicated_merge_tree", False)
        )

        def count_lines_with(lines, pattern):
            return sum([1 for x in lines if pattern in x])

        remore_pathes_with_old_format = count_lines_with(
            remote_pathes_old.split(), "old-style-prefix"
        )
        remore_pathes_with_new_format = count_lines_with(
            remote_pathes_old.split(), "new-style-prefix"
        )

        if storage_policy == "s3_template_key":
            assert remore_pathes_with_old_format == remore_pathes_with_new_format
            assert remore_pathes_with_old_format == len(remote_pathes_old.split()) / 2
        else:
            assert remore_pathes_with_old_format == len(remote_pathes_old.split())
            assert remore_pathes_with_new_format == 0

        parts = (
            node_old.query(
                """
                SELECT name
                FROM system.parts
                WHERE
                    table = 'test_replicated_merge_tree'
                    AND active
                ORDER BY ALL
                """
            )
            .strip()
            .split()
        )
        table_shared_uuid = node_old.query(
            f"SELECT value FROM system.zookeeper WHERE path='{zk_table_path}' and name='table_shared_id'"
        ).strip()

        part_blobs = {}
        blobs_replicas = {}

        for part in parts:
            blobs = (
                node_old.query(
                    f"""
                    SELECT name
                    FROM system.zookeeper
                    WHERE path='/clickhouse/zero_copy/zero_copy_s3/{table_shared_uuid}/{part}'
                    ORDER BY ALL
                    """
                )
                .strip()
                .split()
            )

            for blob in blobs:
                replicas = (
                    node_old.query(
                        f"""
                        SELECT name
                        FROM system.zookeeper
                        WHERE path='/clickhouse/zero_copy/zero_copy_s3/{table_shared_uuid}/{part}/{blob}'
                        ORDER BY ALL
                        """
                    )
                    .strip()
                    .split()
                )
                assert blob not in blobs_replicas
                blobs_replicas[blob] = replicas

            assert part not in part_blobs
            part_blobs[part] = blobs

        assert len(parts) == 2, "parts: " + str(parts)
        assert len(part_blobs.keys()) == len(parts), (
            "part_blobs: " + str(part_blobs) + "; parts: " + str(parts)
        )
        assert len(blobs_replicas.keys()) == len(parts), (
            "blobs_replicas: " + str(blobs_replicas) + "; parts: " + str(parts)
        )

        for replicas in blobs_replicas.values():
            assert len(replicas) == 2, "blobs_replicas: " + str(blobs_replicas)

        for blob in blobs_replicas.keys():
            assert re.match(
                "(old-style-prefix_with-several-section|[a-z]{3}-first-random-part_new-style-prefix_constant-part)_[a-z]{3}_[a-z]{29}",
                blob,
            ), "blobs_replicas: " + str(blobs_replicas)

        old_style_count = sum(
            [1 for x in blobs_replicas.keys() if "old-style-prefix" in x]
        )
        new_style_count = sum(
            [1 for x in blobs_replicas.keys() if "new-style-prefix" in x]
        )

        assert (new_style_count > 0 and old_style_count == new_style_count) or (
            new_style_count == 0 and old_style_count == len(blobs_replicas)
        )


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
