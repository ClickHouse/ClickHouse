#!/usr/bin/env python3

# Regression test: a transient Keeper error must not fail DROP DATABASE of a Replicated database.
#
# DatabaseReplicatedDDLWorker::shutdown() eagerly removes the replica's "active" ephemeral node.
# That removal is a best-effort optimization (it only makes SYSTEM DROP DATABASE REPLICA prompt),
# but ZooKeeper::tryRemove throws on a hardware Keeper error and nothing on the DROP path caught
# it, so DROP DATABASE ... SYNC failed with KEEPER_EXCEPTION and left the database attached.
#
# The failpoint injects that exact error (ZCONNECTIONLOSS on the "active" path) at the reported
# site, so the scenario is deterministic instead of racing a real Keeper.

import time

import pytest

from helpers.cluster import ClickHouseCluster

FAILPOINT = "database_replicated_fail_active_node_removal_on_shutdown"
INJECTED = "Injected Keeper error while removing the active node"
BEST_EFFORT_LOG = "Failed to stop replication of database"

# The same site, but with a user-class Keeper error instead of a hardware one.
NONRETRYABLE_FAILPOINT = "database_replicated_fail_active_node_removal_nonretryable"
NONRETRYABLE_INJECTED = (
    "Injected non-retryable Keeper error while removing the active node"
)

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _db_path(db):
    return f"/clickhouse/databases/{db}"


def _replica_path(db):
    return f"{_db_path(db)}/replicas/shard1|replica1"


def _create_replicated_database(db):
    node.query(
        f"CREATE DATABASE {db} "
        f"ENGINE = Replicated('{_db_path(db)}', 'shard1', 'replica1')"
    )
    node.query(f"CREATE TABLE {db}.t (a UInt64) ENGINE = MergeTree ORDER BY a")
    # The DDL worker must be up, otherwise there is no "active" node to fail on removing.
    zk = cluster.get_kazoo_client("zoo1")
    assert zk.exists(f"{_replica_path(db)}/active") is not None


def _truncate_log():
    node.exec_in_container(
        ["bash", "-c", ": > /var/log/clickhouse-server/clickhouse-server.log"]
    )


def _settled_count_in_log(substring, timeout=30, quiet=1.0, poll=0.25):
    # query_and_get_error returns as soon as the server has SENT the exception, while the
    # remaining log records for that same throw are written afterwards. Reading the count right
    # away can therefore observe a partially written set. Wait until it stops growing instead of
    # assuming how many records one throw emits, which is what keeps the caller's exact-equality
    # assertion from depending on that number.
    deadline = time.monotonic() + timeout
    count = int(node.count_in_log(substring))
    stable_since = time.monotonic()
    while time.monotonic() < deadline:
        time.sleep(poll)
        current = int(node.count_in_log(substring))
        if current != count:
            count = current
            stable_since = time.monotonic()
            continue
        if time.monotonic() - stable_since >= quiet:
            return count
    raise Exception(
        f"The count of '{substring}' in the log did not settle within {timeout}s "
        f"(last value {count})"
    )


def _database_count(db):
    return node.query(
        f"SELECT count() FROM system.databases WHERE name = '{db}'"
    ).strip()


def test_drop_database_survives_keeper_error_on_active_node_removal(started_cluster):
    db = "rdb_drop_keeper_error"
    _create_replicated_database(db)
    zk = cluster.get_kazoo_client("zoo1")
    _truncate_log()

    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    try:
        # Before the fix this failed with
        #   Code: 999. Coordination::Exception: Connection loss, path .../active
        node.query(f"DROP DATABASE {db} SYNC")
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")

    # The injected error must have fired, otherwise the test proves nothing.
    assert node.grep_in_log(INJECTED, only_latest=True), (
        "The injected Keeper error did not fire, so the DROP did not exercise the failing path."
    )
    # And it must have been handled as best-effort rather than escaping the statement.
    assert node.grep_in_log(BEST_EFFORT_LOG, only_latest=True), (
        "The Keeper error was not logged as a best-effort failure by the DROP."
    )

    # The database is really gone, both locally and in Keeper.
    assert _database_count(db) == "0"
    assert zk.exists(_db_path(db)) is None

    # DatabaseCatalog::detachDatabase calls db->shutdown() a second time during the same DROP.
    # The first failed shutdown must have released its holders, so the second call short-circuits
    # and the removal is attempted exactly once. Without that, the second attempt throws again and
    # DatabaseReplicated::shutdown rethrows it, failing the DROP.
    assert int(node.count_in_log(INJECTED)) == 1, (
        "The active node removal was attempted more than once for a single DROP: the failed "
        "shutdown did not release its holders."
    )


def test_detach_database_removes_active_node_despite_keeper_error(started_cluster):
    # setAlreadyRemoved() must stay on the success path, so that when the eager removal fails
    # ~EphemeralNodeHolder still retries it (guarded). Observed on DETACH, which keeps the
    # database's Keeper nodes around and so lets the "active" node be inspected afterwards.
    db = "rdb_detach_retry"
    _create_replicated_database(db)
    zk = cluster.get_kazoo_client("zoo1")
    active = f"{_replica_path(db)}/active"

    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    try:
        # DETACH is deliberately not best-effort, so it still reports the error (see below).
        node.query_and_get_error(f"DETACH DATABASE {db}")
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")

    assert zk.exists(active) is None, (
        "The active node survived a failed eager removal: setAlreadyRemoved() must not run on "
        "the failure path, so that ~EphemeralNodeHolder retries the removal."
    )

    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_detach_database_still_reports_keeper_error(started_cluster):
    # Must-not-regress: the best-effort handling is DROP-only. A DETACH keeps the database on
    # disk to be reattached, so a failure to stop replication must still surface.
    db = "rdb_detach_keeper_error"
    _create_replicated_database(db)

    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    try:
        error = node.query_and_get_error(f"DETACH DATABASE {db}")
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")

    assert INJECTED in error, f"DETACH swallowed the Keeper error: {error}"
    assert _database_count(db) == "1", "The database was detached despite the error."

    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_drop_database_still_reports_non_hardware_keeper_error(started_cluster):
    # Must-not-regress: the best-effort handling is narrowed to HARDWARE Keeper errors, which are
    # transient and release the ephemeral node when the session expires anyway. ZBADVERSION is
    # injected because it is a user-class (non-hardware) error, so it must still fail the DROP
    # instead of being swallowed.
    db = "rdb_drop_nonretryable"
    _create_replicated_database(db)

    node.query(f"SYSTEM ENABLE FAILPOINT {NONRETRYABLE_FAILPOINT}")
    try:
        error = node.query_and_get_error(f"DROP DATABASE {db} SYNC")
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {NONRETRYABLE_FAILPOINT}")

    assert NONRETRYABLE_INJECTED in error, (
        f"DROP swallowed a non-hardware Keeper error: {error}"
    )
    # The DROP genuinely failed rather than half-succeeding.
    assert _database_count(db) == "1", "The database was dropped despite the error."

    node.query(f"DROP DATABASE IF EXISTS {db} SYNC")


def test_detach_database_removes_active_node_on_healthy_session(started_cluster):
    # Must-not-regress: with a healthy session the eager removal in
    # DatabaseReplicatedDDLWorker::shutdown() still happens, so SYSTEM DROP DATABASE REPLICA does
    # not spuriously report the replica as active.
    #
    # Observed on DETACH, not DROP: DatabaseCatalog::detachDatabase calls db->drop() only when
    # dropping, and DatabaseReplicated::drop removes the whole replica path recursively. So on a
    # DROP the active node is gone either way and the eager removal is unobservable.
    db = "rdb_detach_healthy"
    _create_replicated_database(db)
    zk = cluster.get_kazoo_client("zoo1")
    active = f"{_replica_path(db)}/active"
    replica = _replica_path(db)

    # No failpoint here: this is the healthy session path.
    node.query(f"DETACH DATABASE {db}")

    assert zk.exists(active) is None, (
        "The eager removal in DatabaseReplicatedDDLWorker::shutdown did not remove the active "
        "node on a healthy session (the behaviour added in 7ecd310)."
    )
    # Nothing removed the replica path itself, so the active node's absence above can only be
    # attributed to the shutdown path.
    assert zk.exists(replica) is not None, (
        "The replica path was removed, so this test cannot attribute the missing active node to "
        "the eager removal."
    )
    assert _database_count(db) == "0"

    node.query(f"ATTACH DATABASE {db}")
    node.query(f"DROP DATABASE {db} SYNC")


def test_drop_database_removes_active_node_on_healthy_session(started_cluster):
    # Must-not-regress: a healthy DROP takes neither the injected error nor the best-effort
    # branch. The eager removal itself is asserted by
    # test_detach_database_removes_active_node_on_healthy_session above, because
    # DatabaseReplicated::drop removes the replica path recursively and would mask it here.
    db = "rdb_drop_healthy"
    _create_replicated_database(db)
    zk = cluster.get_kazoo_client("zoo1")
    _truncate_log()

    node.query(f"DROP DATABASE {db} SYNC")

    assert _database_count(db) == "0"
    assert zk.exists(_db_path(db)) is None
    assert not node.grep_in_log(INJECTED, only_latest=True)
    # Nothing was best-effort about this DROP: replication stopped cleanly.
    assert not node.grep_in_log(BEST_EFFORT_LOG, only_latest=True)


def test_restore_database_replica_after_failed_shutdown(started_cluster):
    # Must-not-regress: SYSTEM RESTORE DATABASE REPLICA reaches the same
    # DatabaseReplicatedDDLWorker::shutdown() through reinitializeDDLWorker. A restore that hits
    # the Keeper error still fails, as before the fix, but it must not leave the DDL worker in a
    # state that blocks a later successful restore.
    db = "rdb_restore"
    _create_replicated_database(db)
    zk = cluster.get_kazoo_client("zoo1")

    # A restore only proceeds when the replica's nodes are missing from Keeper, otherwise
    # createReplicaNodesInZooKeeper reports REPLICA_ALREADY_EXISTS. The database stays attached so
    # that its DDL worker is alive and reinitializeDDLWorker really shuts it down.
    zk.delete(_replica_path(db), recursive=True)

    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    try:
        error = node.query_and_get_error(f"SYSTEM RESTORE DATABASE REPLICA {db}")
        assert INJECTED in error, (
            f"The restore did not reach the injected removal: {error}"
        )
        # Sampled only once the first throw's records have settled, so the equality below cannot
        # fail just because a record landed between the two reads.
        before = _settled_count_in_log(INJECTED)

        # The failpoint is deliberately still armed. reinitializeDDLWorker throws before it can
        # reset ddl_worker, so this second restore shuts the same stale worker down again. It can
        # only succeed if the failed shutdown released its holders, because otherwise the removal
        # is re-entered and throws again.
        zk.delete(_replica_path(db), recursive=True)
        node.query(f"SYSTEM RESTORE DATABASE REPLICA {db}")
        # Exact equality, never before + N: count_in_log counts log lines, and an unhandled throw
        # on the restore path emits two of them (executeQuery and TCPHandler). Settled on this
        # side too, so a regression that does re-enter the removal is given time to log rather
        # than slipping past the read.
        assert _settled_count_in_log(INJECTED) == before, (
            "The second restore re-entered the eager removal: the failed shutdown did not "
            "release its holders."
        )
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")

    node.query(f"CREATE TABLE {db}.t2 (a UInt64) ENGINE = MergeTree ORDER BY a")
    assert node.query(f"SELECT count() FROM {db}.t2").strip() == "0"

    node.query(f"DROP DATABASE {db} SYNC")
