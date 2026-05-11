import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_rmv_access_denied_on_rename_race(start_cluster):
    """
    Reproduces the race between an upstream REPLACE refreshable materialized view's
    EXCHANGE and a downstream APPEND refreshable materialized view whose analyzer-
    level table resolution touches the same target.

    On each refresh the upstream RMV renames its target storage in memory via
    EXCHANGE (the in-memory StorageID briefly becomes the name of the temporary
    counterpart). If the downstream resolver captures the storage pointer before
    the EXCHANGE and constructs the query tree's TableNode after it, the
    TableNode caches the post-rename identity. Because the downstream runs under
    SQL SECURITY DEFINER with grants bound to the real table name, the
    subsequent access check fails against a name the user was never granted.

    This is the engine-agnostic variant of the race, using a default Atomic
    database and a plain MergeTree engine. A SharedCatalog-specific variant
    with a more aggressive drop-to-staging window lives in
    test_rmv_access_denied_during_exchange.
    """
    node.query("DROP DATABASE IF EXISTS test_rmv SYNC")
    node.query("DROP USER IF EXISTS rmv_definer")

    node.query("CREATE DATABASE test_rmv")

    node.query(
        """
        CREATE TABLE test_rmv.src
        (
            k Int32,
            v Int32
        )
        ENGINE = MergeTree
        ORDER BY k
        """
    )
    node.query(
        "INSERT INTO test_rmv.src SELECT number, number * 2 FROM numbers(1000)"
    )

    # Explicit target table for the upstream RMV. The EXCHANGE on each refresh
    # swaps this exact table by user-facing name, which is what the downstream
    # reads — a target -> target chain of refreshable views.
    node.query(
        """
        CREATE TABLE test_rmv.ups_target
        (
            k Int32,
            v Int32
        )
        ENGINE = MergeTree
        ORDER BY k
        """
    )

    # Upstream RMV: REPLACE mode every 1 second -> each refresh invokes
    # exchangeTargetTable + dropTempTable, briefly mutating the in-memory
    # StorageID of the old ups_target storage.
    node.query(
        """
        CREATE MATERIALIZED VIEW test_rmv.ups_rmv
        REFRESH EVERY 1 SECOND
        TO test_rmv.ups_target AS
        SELECT k, v FROM test_rmv.src
        """
    )

    # Downstream target table and the definer user. Grants are column-scoped on
    # real table names only; no wildcard that would mask a race manifesting as
    # ACCESS_DENIED against a transient post-rename identity.
    node.query(
        """
        CREATE TABLE test_rmv.downstream_tgt
        (
            k Int32,
            v Int32
        )
        ENGINE = MergeTree
        ORDER BY k
        """
    )
    node.query("CREATE USER rmv_definer IDENTIFIED WITH no_password")
    node.query("GRANT SELECT(k, v) ON test_rmv.ups_target TO rmv_definer")
    node.query("GRANT INSERT(k, v) ON test_rmv.downstream_tgt TO rmv_definer")

    # Downstream RMV: APPEND + SQL SECURITY DEFINER using the column-scoped user.
    # Reads ups_target by name concurrently with the upstream's EXCHANGE.
    node.query(
        """
        CREATE MATERIALIZED VIEW test_rmv.dn
        REFRESH EVERY 1 SECOND APPEND
        TO test_rmv.downstream_tgt
        DEFINER = rmv_definer SQL SECURITY DEFINER
        AS SELECT k, v FROM test_rmv.ups_target
        """
    )

    # Let refreshes stabilize, then drive the race by forcing frequent SYSTEM
    # REFRESHes on both views. Natural 1-second refreshes alone produce too few
    # race windows in a 60-second test run.
    time.sleep(2)
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            node.query("SYSTEM REFRESH VIEW test_rmv.ups_rmv")
            node.query("SYSTEM REFRESH VIEW test_rmv.dn")
        except Exception:
            pass
        time.sleep(0.05)

    node.query("SYSTEM FLUSH LOGS query_log")

    # Collect any refresh failures caused by the race in the analyzer's table
    # resolver. Depending on which point of the upstream's EXCHANGE cycle the
    # downstream resolver caught, the same race manifests as either:
    #   * Code 497 ACCESS_DENIED citing a transient post-rename table name —
    #     the storage's in-memory StorageID was mutated mid-resolution and the
    #     cached identity no longer matches the user's grant.
    #   * Code 60 UNKNOWN_TABLE citing "<db>.<table> (<uuid>) does not exist" —
    #     the resolved UUID was purged from the catalog between resolveStorageID
    #     and the subsequent access, and the fallback name lookup also failed.
    # rmv_definer has sufficient grants for the legitimate refresh path, so any
    # 497 or 60 under a test_rmv refresh log_comment is evidence of the race.
    offending_rows = node.query(
        """
        SELECT event_time, exception_code, query_id, log_comment, exception
        FROM system.query_log
        WHERE type != 'QueryStart'
          AND exception_code IN (60, 497)
          AND log_comment LIKE 'refresh of test_rmv.%'
        FORMAT Vertical
        """
    )

    node.query("DROP DATABASE IF EXISTS test_rmv SYNC")
    node.query("DROP USER IF EXISTS rmv_definer")

    assert offending_rows == "", (
        "Refresh failed with an error indicating the TableNode was constructed "
        "with a post-rename storage identity instead of the user-requested one. "
        "Expected no ACCESS_DENIED (497) or UNKNOWN_TABLE (60) errors under "
        "refresh log comments, saw:\n" + offending_rows
    )
