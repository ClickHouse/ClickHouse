import os
import shlex
from pathlib import Path

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
old_nodes = tuple(
    cluster.add_instance(f"old{i}", with_zookeeper=True, stay_alive=True)
    for i in range(1, 4)
)
new_nodes = tuple(
    cluster.add_instance(f"new{i}", with_zookeeper=True, stay_alive=True)
    for i in range(1, 4)
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def table_path(node, table):
    return node.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()


def active_parts(node, table):
    return node.query(
        "SELECT name FROM system.parts "
        f"WHERE database = 'default' AND table = '{table}' AND active ORDER BY name"
    ).splitlines()


def part_headers(node, zk_path, replica):
    return node.query(
        f"SELECT name, hex(value) FROM system.zookeeper WHERE path = '{zk_path}/replicas/{replica}/parts' ORDER BY name"
    )


def replica_state(node, zk_path, replica):
    return node.query(
        f"SELECT name, value FROM system.zookeeper WHERE path = '{zk_path}/replicas/{replica}' "
        "AND name IN ('is_lost', 'log_pointer') ORDER BY name"
    )


def keeper_counter(node, path, name):
    return node.query(
        f"SELECT cversion FROM system.zookeeper WHERE path = '{path}' AND name = '{name}'"
    )


def successful_fetches(node):
    return node.query(
        "SELECT sum(value) FROM system.events WHERE event = 'ReplicatedPartFetches'"
    )


def backup_parts(node, table, parts):
    archive = f"/var/lib/clickhouse/tmp/{table}_backup.tar"
    node.exec_in_container(
        ["tar", "-cf", archive, "-C", table_path(node, table), *parts], user="root"
    )
    local_archive = Path(cluster.instances_dir) / "tmp" / f"{table}_backup.tar"
    local_archive.parent.mkdir(parents=True, exist_ok=True)
    node.copy_file_from_container(archive, str(local_archive))
    return local_archive


def stage_backup(node, table, parts, local_archive):
    archive = f"/var/lib/clickhouse/tmp/{table}_restore.tar"
    node.copy_file_to_container(str(local_archive), archive)
    detached = os.path.join(table_path(node, table), "detached")
    staging = os.path.join(detached, "tmp_restore")
    node.exec_in_container(
        ["bash", "-ec", f"mkdir -p {shlex.quote(staging)}; tar -xf {shlex.quote(archive)} -C {shlex.quote(staging)}"],
        user="root",
    )
    for part in parts:
        target = os.path.join(detached, part)
        node.exec_in_container(
            ["bash", "-ec", f"test ! -e {shlex.quote(target)}; mv -T {shlex.quote(os.path.join(staging, part))} {shlex.quote(target)}"],
            user="root",
        )
    node.exec_in_container(["rmdir", staging], user="root")


def test_bootstrap_after_all_existing_replicas_die(started_cluster):
    table = "zone_failure"
    zk_path = "/clickhouse/tables/reuse_detached_zone_failure"
    source = old_nodes[0]
    observer = new_nodes[0]
    for node in old_nodes:
        node.query(
            f"CREATE TABLE {table} (x UInt64) "
            f"ENGINE = ReplicatedMergeTree('{zk_path}', '{node.name}') ORDER BY x;"
            f"SYSTEM STOP MERGES {table}"
        )
    source.query(
        f"INSERT INTO {table} VALUES (111);"
        f"INSERT INTO {table} VALUES (222);"
        f"INSERT INTO {table} VALUES (333)"
    )
    parts = active_parts(source, table)
    assert len(parts) == 3
    for node in old_nodes:
        node.query(f"SYSTEM SYNC REPLICA {table}")
        assert_eq_with_retry(
            node,
            "SELECT is_readonly, is_session_expired, active_replicas, total_replicas, queue_size "
            f"FROM system.replicas WHERE database = 'default' AND table = '{table}'",
            "0\t0\t3\t3\t0",
            retry_count=120,
        )
        assert active_parts(node, table) == parts
    headers = {node.name: part_headers(source, zk_path, node.name) for node in old_nodes}
    states = {node.name: replica_state(source, zk_path, node.name) for node in old_nodes}
    log_version = keeper_counter(source, zk_path, "log")
    block_version = keeper_counter(source, f"{zk_path}/block_numbers", "all")
    backup = backup_parts(source, table, parts)

    # Preserve Keeper registrations, but kill every old ClickHouse server without shutdown.
    for node in old_nodes:
        node.stop_clickhouse(kill=True)
        assert node.get_process_pid("clickhouse") is None
    for node in old_nodes:
        assert_eq_with_retry(
            observer,
            f"SELECT count() FROM system.zookeeper WHERE path = '{zk_path}/replicas/{node.name}' AND name = 'is_active'",
            "0",
            retry_count=240,
        )
        assert part_headers(observer, zk_path, node.name) == headers[node.name]
        assert replica_state(observer, zk_path, node.name) == states[node.name]
        assert observer.query(
            f"SELECT value FROM system.zookeeper WHERE path = '{zk_path}/replicas/{node.name}' AND name = 'is_lost'"
        ) == "0\n"

    fetch_counts = {}
    for node in new_nodes:
        node.query(
            f"CREATE TABLE {table} (x UInt64) "
            f"ENGINE = ReplicatedMergeTree('{zk_path}', '{node.name}') ORDER BY x "
            "SETTINGS max_postpone_time_for_failed_replicated_tasks_ms = 0;"
            f"SYSTEM STOP FETCHES {table};"
            f"SYSTEM STOP MERGES {table};"
            f"SYSTEM STOP REPLICATED SENDS {table}"
        )
        assert_eq_with_retry(
            node,
            "SELECT count(), countIf(is_currently_executing) FROM system.replication_queue "
            f"WHERE database = 'default' AND table = '{table}'",
            "3\t0",
            retry_count=120,
        )
        assert active_parts(node, table) == []
        assert part_headers(node, zk_path, node.name) == ""
        assert node.query(
            "SELECT type, new_part_name FROM system.replication_queue "
            f"WHERE database = 'default' AND table = '{table}' ORDER BY new_part_name"
        ) == "".join(f"ATTACH_PART\t{part}\n" for part in parts)
        fetch_counts[node.name] = successful_fetches(node)
        stage_backup(node, table, parts, backup)

    # Keep all replacement peer sends stopped: each replica must attach its own backup.
    for node in new_nodes:
        node.query(f"SYSTEM START FETCHES {table}")
    for node in new_nodes:
        node.query(f"SYSTEM SYNC REPLICA {table}")
        assert node.query(f"SELECT x FROM {table} ORDER BY x") == "111\n222\n333\n"
        assert active_parts(node, table) == parts
        assert part_headers(node, zk_path, node.name) == headers[source.name]
        assert node.query(
            f"SELECT count() FROM system.detached_parts WHERE database = 'default' AND table = '{table}'"
        ) == "0\n"
        assert successful_fetches(node) == fetch_counts[node.name]
        assert node.query(
            "SELECT is_readonly, active_replicas, total_replicas, queue_size, lost_part_count "
            f"FROM system.replicas WHERE database = 'default' AND table = '{table}'"
        ) == "0\t3\t6\t0\t0\n"
    assert keeper_counter(observer, zk_path, "log") == log_version
    assert keeper_counter(observer, f"{zk_path}/block_numbers", "all") == block_version
    for node in old_nodes:
        assert node.get_process_pid("clickhouse") is None
        assert part_headers(observer, zk_path, node.name) == headers[node.name]
        assert replica_state(observer, zk_path, node.name) == states[node.name]

    # Restore the shared cluster only after asserting recovery with every old server dead.
    for node in old_nodes:
        node.start_clickhouse()
    for node in (*old_nodes, *new_nodes):
        node.query(f"DROP TABLE {table} SYNC")
