#!/usr/bin/env python3

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/106737
#
# A coordinated refreshable materialized view (created in a Replicated database) used to
# crash the server when it was re-attached on a Keeper that does not support the MULTI_READ
# feature flag. The constructor's up-front MULTI_READ check was gated behind the fresh-CREATE
# path only, so on ATTACH/restore the view became coordinated=true and the scheduling thread
# later threw NOT_IMPLEMENTED in readZnodesIfNeeded, which the doScheduling catch-all re-raised
# as a LOGICAL_ERROR -> server abort (and crash-loop on restart). The view must instead stop
# gracefully and leave the server running.

import os
import time

import pytest

from helpers.cluster import ClickHouseCluster

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))

cluster = ClickHouseCluster(__file__)

# The keeper config is swapped at runtime (see use_keeper_config), so it is installed from the
# test body rather than as a main_config that the harness would also try to manage.
node = cluster.add_instance(
    "node",
    user_configs=["configs/settings.xml"],
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def use_keeper_config(config_name):
    """Install one of the keeper configs as the single enable_keeper.xml on the node."""
    node.copy_file_to_container(
        os.path.join(CURRENT_TEST_DIR, "configs", config_name),
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
    )


def negotiated_multi_read():
    """MULTI_READ as negotiated by the server's current Keeper session: True/False, or None
    if there is no Keeper session yet (system.zookeeper_connection empty)."""
    # Touch system.zookeeper to force the server to open its (lazy) Keeper session.
    node.query(
        "SELECT count() FROM system.zookeeper WHERE path = '/'", ignore_error=True
    )
    rows = node.query(
        "SELECT has(arrayMap(x -> toString(x), enabled_feature_flags), 'MULTI_READ') "
        "FROM system.zookeeper_connection WHERE name = 'default'"
    ).strip()
    if rows == "":
        return None
    return rows.splitlines()[0] == "1"


def restart_with_keeper_config(config_name, want_multi_read):
    """Switch the embedded Keeper config and restart, then wait until the server's Keeper
    session has actually negotiated the wanted MULTI_READ state before returning.

    The embedded Keeper restarts together with the server, and on a loaded host the Keeper
    can still be applying the new feature_flags config (re-deriving the /keeper/feature_flags
    system znode) while it already accepts client connections. If the server's ZooKeeper
    client negotiates feature flags in that window, it caches the stale value for the whole
    session - and a coordinated refreshable MV reads MULTI_READ only once, at attach. So
    without this barrier the view could attach against a stale MULTI_READ=enabled view of a
    Keeper that is actually configured without it, making the test flaky (observed only on
    slow CI environments). Restart until the live session's negotiated flag matches the config.
    """
    deadline = time.monotonic() + 180
    while True:
        use_keeper_config(config_name)
        node.restart_clickhouse()
        # The server must be up and answering queries (no crash, no crash-loop).
        assert node.query("SELECT 1").strip() == "1"
        # Wait for an actual Keeper session, then check the negotiated flag (None = no session yet).
        while time.monotonic() < deadline:
            flag = negotiated_multi_read()
            if flag is not None:
                break
            time.sleep(0.5)
        if flag == want_multi_read:
            return
        assert (
            time.monotonic() < deadline
        ), f"Keeper session never negotiated MULTI_READ={want_multi_read} after restart"


def test_refreshable_mv_attach_without_multi_read(started_cluster):
    restart_with_keeper_config("enable_keeper_multi_read.xml", want_multi_read=True)

    node.query(
        "CREATE DATABASE rdb ENGINE = Replicated('/clickhouse/rdb', '{shard}', '{replica}')"
    )
    # Non-APPEND refreshable MV in a Replicated database is always coordinated.
    node.query("""
        CREATE MATERIALIZED VIEW rdb.mv
        REFRESH EVERY 1 SECOND
        ENGINE = ReplicatedMergeTree ORDER BY x
        EMPTY
        AS SELECT number AS x FROM numbers(3)
        """)

    # Refresh once while MULTI_READ is available, so coordination znodes exist and the view's
    # persisted state is the normal coordinated one.
    node.query("SYSTEM REFRESH VIEW rdb.mv")
    node.query("SYSTEM WAIT VIEW rdb.mv")
    assert node.query("SELECT count() FROM rdb.mv").strip() == "3"

    # Downgrade Keeper: disable MULTI_READ, then restart. On startup the Replicated database
    # re-attaches rdb.mv (attach=true), which is exactly the path that used to crash. Wait until
    # the server's Keeper session actually sees MULTI_READ gone before checking the view, so the
    # re-attach is guaranteed to happen against the no-multi-read Keeper.
    restart_with_keeper_config("enable_keeper_no_multi_read.xml", want_multi_read=False)

    # Attach schedules the graceful-stop pass asynchronously, so the status read can otherwise
    # observe the transient Scheduling state before doScheduling reaches the Disabled state. Wait
    # for the scheduled pass to settle (WAIT VIEW returns immediately once the view is Disabled).
    node.query("SYSTEM WAIT VIEW rdb.mv")

    # The view must have stopped gracefully, reporting the reason rather than aborting.
    status = node.query(
        "SELECT status, exception FROM system.view_refreshes WHERE view = 'mv'"
    )
    assert "Disabled" in status, status
    assert "multi-read" in status.lower() or "multi_read" in status.lower(), status

    # The doScheduling catch-all LOGICAL_ERROR must not have fired.
    assert not node.contains_in_log("Unexpected exception in refresh scheduling")

    # The gracefully-stopped state must be non-resumable: a coordinated view must never be turned
    # into an uncoordinated local refresh (that would corrupt the replicated target table). So
    # SYSTEM START VIEW must NOT resume it while MULTI_READ is still missing - it stays Disabled,
    # runs no refresh, and the server stays up.
    node.query("SYSTEM START VIEW rdb.mv")
    node.query("SYSTEM REFRESH VIEW rdb.mv")
    assert node.query("SELECT 1").strip() == "1"
    # SYSTEM REFRESH VIEW is async: run() moves the task to Scheduling, and only the background
    # scheduler later hits the coordination.unavailable branch and switches it back to Disabled.
    # Wait for that scheduled pass before asserting, so the read never observes transient Scheduling.
    node.query("SYSTEM WAIT VIEW rdb.mv")
    status = node.query(
        "SELECT status FROM system.view_refreshes WHERE view = 'mv'"
    ).strip()
    assert status == "Disabled", status
    assert not node.contains_in_log("Unexpected exception in refresh scheduling")

    # Restoring MULTI_READ and restarting must keep the server healthy.
    restart_with_keeper_config("enable_keeper_multi_read.xml", want_multi_read=True)

    node.query("DROP DATABASE rdb SYNC")
