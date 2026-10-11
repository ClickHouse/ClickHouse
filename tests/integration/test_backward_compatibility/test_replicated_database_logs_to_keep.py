# A `Replicated` database must stay safe for replicas still running a release whose DDL worker evaluates
# `entry_number + logs_to_keep < max_log_ptr` in 32-bit arithmetic, see
# https://github.com/ClickHouse/ClickHouse/issues/122377. This holds both for a database created by the
# current server and for one created while the maximum was `UInt32::max`, whose Keeper node the current
# server rewrites.

import time
import uuid

import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# `logs_to_keep` used to be 64-bit, so a config may hold a value above the maximum; the current server
# clamps it and writes the clamped value to Keeper when it creates a database. The old server gets no
# config: it only reads the value from Keeper, and not every old release knows the config key.
node_new = cluster.add_instance(
    "node_new",
    main_configs=["configs/database_replicated_logs_to_keep_overflow.xml"],
    with_zookeeper=True,
)
node_old = cluster.add_instance(
    "node_old",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def log_entries(path):
    return int(
        node_new.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{path}/log'"
        ).strip()
    )


def wait_for_more_log_lines(node, substring, previous_count):
    for _ in range(60):
        if int(node.count_in_log(substring)) > previous_count:
            return
        time.sleep(0.5)
    raise TimeoutError(f"'{substring}' did not appear in the log")


def test_old_replica_of_database_created_by_new_server(start_cluster):
    db = f"db_{uuid.uuid4().hex[:8]}"
    path = f"/clickhouse/databases/{db}"

    node_new.query(f"CREATE DATABASE {db} ENGINE = Replicated('{path}', 's1', 'new')")
    assert (
        node_new.query(
            f"SELECT value FROM system.zookeeper WHERE path = '{path}' AND name = 'logs_to_keep'"
        ).strip()
        == "2147483647"
    )

    node_old.query(f"CREATE DATABASE {db} ENGINE = Replicated('{path}', 's1', 'old')")
    # The DDL is issued by the old server, so that both servers understand the query text. Each query
    # waits until both replicas execute it, so the old replica is in sync afterwards.
    for i in range(3):
        node_old.query(f"CREATE TABLE {db}.t{i} (x UInt32) ENGINE = MergeTree ORDER BY x")

    entries_before = log_entries(path)
    assert entries_before >= 3

    initialized = f"DDLWorker({db}): Initialized DDLWorker thread"
    cleaning = f"DDLWorker({db}): Cleaning queue"
    initialized_before = int(node_old.count_in_log(initialized))
    cleaning_before = int(node_old.count_in_log(cleaning))

    # A re-attached database gets a new DDL worker, which first decides whether the replica is lost,
    # then logs `Initialized DDLWorker thread`, and then runs a cleanup pass right away.
    node_old.query(f"DETACH DATABASE {db}")
    node_old.query(f"ATTACH DATABASE {db}")

    # The replica is in sync, so it must not be declared lost. With `UInt32::max` in Keeper the old
    # check `our_log_ptr + logs_to_keep < max_log_ptr` wraps to `our_log_ptr - 1 < max_log_ptr`.
    wait_for_more_log_lines(node_old, initialized, initialized_before)
    assert int(node_old.count_in_log(f"DDLWorker({db}): Replica seems to be lost")) == 0

    # The cleanup pass must delete nothing. Detaching joins the cleanup thread, so the pass that has
    # started is over by the time the entries are counted.
    wait_for_more_log_lines(node_old, cleaning, cleaning_before)
    node_old.query(f"DETACH DATABASE {db}")
    assert log_entries(path) == entries_before

    node_old.query(f"ATTACH DATABASE {db}")
    node_old.query(f"DROP DATABASE {db} SYNC")
    node_new.query(f"DROP DATABASE {db} SYNC")


def test_oversized_logs_to_keep_in_keeper_is_rewritten(start_cluster):
    # A database created while the maximum was `UInt32::max` holds that value in Keeper. The current
    # server must rewrite the node, because older replicas joining later, or after a rollback, read it.
    db = f"db_{uuid.uuid4().hex[:8]}"
    path = f"/clickhouse/databases/{db}"
    logs_to_keep_path = f"{path}/logs_to_keep"

    node_new.query(f"CREATE DATABASE {db} ENGINE = Replicated('{path}', 's1', 'new')")

    zk = cluster.get_kazoo_client("zoo1")
    try:
        node_new.query(f"DETACH DATABASE {db}")
        zk.set(logs_to_keep_path, b"4294967295")

        # A re-attached database gets a new DDL worker, which reads `logs_to_keep` from Keeper when it starts.
        node_new.query(f"ATTACH DATABASE {db}")
        for _ in range(60):
            if zk.get(logs_to_keep_path)[0] == b"2147483647":
                break
            time.sleep(0.5)
        assert zk.get(logs_to_keep_path)[0] == b"2147483647"
        assert node_new.contains_in_log(f"Keeper ({logs_to_keep_path}) held 4294967295")
    finally:
        zk.stop()
        zk.close()
        node_new.query(f"DROP DATABASE IF EXISTS {db} SYNC")
