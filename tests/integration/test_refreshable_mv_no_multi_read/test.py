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


def test_refreshable_mv_attach_without_multi_read(started_cluster):
    use_keeper_config("enable_keeper_multi_read.xml")
    node.restart_clickhouse()

    node.query(
        "CREATE DATABASE rdb ENGINE = Replicated('/clickhouse/rdb', '{shard}', '{replica}')"
    )
    # Non-APPEND refreshable MV in a Replicated database is always coordinated.
    node.query(
        """
        CREATE MATERIALIZED VIEW rdb.mv
        REFRESH EVERY 1 SECOND
        ENGINE = ReplicatedMergeTree ORDER BY x
        EMPTY
        AS SELECT number AS x FROM numbers(3)
        """
    )

    # Refresh once while MULTI_READ is available, so coordination znodes exist and the view's
    # persisted state is the normal coordinated one.
    node.query("SYSTEM REFRESH VIEW rdb.mv")
    node.query("SYSTEM WAIT VIEW rdb.mv")
    assert node.query("SELECT count() FROM rdb.mv").strip() == "3"

    # Downgrade Keeper: disable MULTI_READ, then restart. On startup the Replicated database
    # re-attaches rdb.mv (attach=true), which is exactly the path that used to crash.
    use_keeper_config("enable_keeper_no_multi_read.xml")
    node.restart_clickhouse()

    # The server must be up and answering queries (no crash, no crash-loop).
    assert node.query("SELECT 1").strip() == "1"

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
    use_keeper_config("enable_keeper_multi_read.xml")
    node.restart_clickhouse()
    assert node.query("SELECT 1").strip() == "1"

    node.query("DROP DATABASE rdb SYNC")
