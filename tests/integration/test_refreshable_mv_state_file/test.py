import shlex
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# An uncoordinated refreshable view persists its schedule on the database disk. These tests read and
# edit that file, so they pin `database_disk` to a local disk at a known path, and opt out of
# with_remote_database_disk, which some builds enable by default and which would put the file in
# object storage instead.
node = cluster.add_instance(
    "node",
    main_configs=["configs/database_disk.xml"],
    user_configs=["configs/settings.xml"],
    with_remote_database_disk=False,
    stay_alive=True,
)

DB_DISK_PATH = "/var/lib/clickhouse/disks/db_meta_disk"
DEFAULT_DISK_PATH = "/var/lib/clickhouse"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def find_refresh_state_files(root):
    found = node.exec_in_container(
        ["bash", "-c", f"find {root} -name 'refresh_state.*.txt' 2>/dev/null | sort"]
    ).strip()
    return [line for line in found.split("\n") if line]


def refresh_info(name, column):
    return node.query(
        f"SELECT {column} FROM system.view_refreshes WHERE view = '{name}'"
    ).strip()


def create_daily_rmv(name):
    """A view that refreshes once now and then not again for a day.

    No EMPTY: an EMPTY view re-runs the constructor's "pretend we just refreshed" branch on every
    restart, so it never refreshes at startup and cannot show the bug these tests are about.
    """
    node.query(f"DROP TABLE IF EXISTS {name}")
    node.query(
        f"CREATE MATERIALIZED VIEW {name} REFRESH EVERY 1 DAY (a DateTime, b UInt64) "
        f"ENGINE = MergeTree ORDER BY tuple() AS SELECT now() a, number b FROM numbers(2)"
    )
    return node.query_with_retry(
        f"SELECT last_success_time FROM system.view_refreshes WHERE view = '{name}'",
        check_callback=lambda x: x.strip() not in ("", "\\N"),
    ).strip()


def test_refresh_state_is_written_to_the_database_disk():
    name = "rmv_db_disk"
    create_daily_rmv(name)
    try:
        assert len(find_refresh_state_files(DB_DISK_PATH)) == 1
        # The view's data is on the default disk; only its schedule goes to the database disk.
        # Using the default *data* policy instead would break servers whose data disk is write-once,
        # where the state could never be published at all.
        outside = [
            path
            for path in find_refresh_state_files(DEFAULT_DISK_PATH)
            if not path.startswith(DB_DISK_PATH)
        ]
        assert outside == []
    finally:
        node.query(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param("not a refresh state at all", id="garbage"),
        pytest.param(
            "format version: 1\nlast_completed_timeslot: 1758240000\n", id="truncated"
        ),
    ],
)
def test_unusable_persisted_refresh_state_stops_the_view(payload):
    name = "rmv_unusable"
    create_daily_rmv(name)
    state_files = find_refresh_state_files(DB_DISK_PATH)
    assert len(state_files) == 1

    try:
        node.stop_clickhouse()
        node.exec_in_container(
            ["bash", "-c", f"printf '%s' {shlex.quote(payload)} > {state_files[0]}"]
        )
        node.start_clickhouse()

        # An unreadable file means a schedule existed and was lost, so refreshing now would be a
        # guess, and the usual cause of unreadability hits every view at once. An absent file is the
        # opposite case and still refreshes once, which is what the deletion arm of
        # test_refreshable_mat_view's restart coverage relies on.
        node.query_with_retry(
            f"SELECT status FROM system.view_refreshes WHERE view = '{name}'",
            check_callback=lambda x: x.strip() == "Disabled",
        )
        assert refresh_info(name, "last_success_time") == "\\N"
        assert "refresh_state" in refresh_info(name, "exception")
        time.sleep(3)
        assert refresh_info(name, "status") == "Disabled"

        # Recoverable without losing the table: removing the unreadable file restores the normal
        # first-start behaviour.
        node.stop_clickhouse()
        node.exec_in_container(["bash", "-c", f"rm -f {state_files[0]}"])
        node.start_clickhouse()
        resumed = node.query_with_retry(
            f"SELECT last_success_time FROM system.view_refreshes WHERE view = '{name}'",
            check_callback=lambda x: x.strip() not in ("", "\\N"),
        ).strip()
        assert resumed != "\\N"
    finally:
        node.query(f"DROP TABLE IF EXISTS {name}")
