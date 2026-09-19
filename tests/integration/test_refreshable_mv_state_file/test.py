import os
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
SERVER_LOG = "/var/log/clickhouse-server/clickhouse-server.log"


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


def read_state_file(path):
    return node.exec_in_container(["bash", "-c", f"cat {shlex.quote(path)}"])


def refresh_info(name, column):
    return node.query(
        f"SELECT {column} FROM system.view_refreshes WHERE view = '{name}'"
    ).strip()


def profile_event(name):
    return int(
        node.query(
            f"SELECT sum(value) FROM system.events WHERE event = '{name}'"
        ).strip()
    )


def wait_for_refresh_info(name, column, predicate):
    """Poll one system.view_refreshes column until `predicate` holds, then assert that it does.

    query_with_retry returns its last result even when the callback never passed, so the assert is
    what makes this a check rather than a delay.
    """
    value = node.query_with_retry(
        f"SELECT {column} FROM system.view_refreshes WHERE view = '{name}'",
        check_callback=lambda x: predicate(x.strip()),
        retry_count=120,
    ).strip()
    assert predicate(value), f"{name}.{column} is {value!r}"
    return value


def count_in_server_logs(pattern):
    """Count matches across the current log and every rotated one.

    A restart rotates the log, so a byte offset taken before it points past the end of the file
    that exists afterwards and would match nothing at all. Comparing this count before and after
    is what makes "the line did not appear" an assertion instead of a tautology.
    """
    return int(
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"zgrep -ac -- {shlex.quote(pattern)} {SERVER_LOG}* 2>/dev/null "
                f"| awk -F: '{{s += $NF}} END {{print s + 0}}'",
            ]
        ).strip()
    )


def create_daily_rmv(name):
    """A view that refreshes once now and then not again for a day.

    No EMPTY: the stored metadata does not keep the EMPTY flag, and an EMPTY view has no completed
    refresh anyway, while last_success_time is what these tests compare across a restart.
    """
    node.query(f"DROP TABLE IF EXISTS {name}")
    node.query(
        f"CREATE MATERIALIZED VIEW {name} REFRESH EVERY 1 DAY (a DateTime, b UInt64) "
        f"ENGINE = MergeTree ORDER BY tuple() AS SELECT now() a, number b FROM numbers(2)"
    )
    return wait_for_refresh_info(
        name, "last_success_time", lambda x: x not in ("", "\\N")
    )


def test_refresh_state_is_written_to_the_database_disk():
    name = "rmv_db_disk"
    create_daily_rmv(name)
    try:
        state_files = find_refresh_state_files(DB_DISK_PATH)
        assert len(state_files) == 1
        # Two replicas of an APPEND view with all_replicas share the table UUID and schedule
        # independently, so the file name has to separate them. serverUUID() is a server-side
        # constant and one data directory holds one of them, so the name cannot collide.
        server_uuid = node.query("SELECT serverUUID()").strip()
        assert os.path.basename(state_files[0]) == f"refresh_state.{server_uuid}.txt"
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


def test_empty_view_keeps_its_schedule_across_restart():
    """An EMPTY view has no refresh to persist its schedule, so startup has to persist it.

    EMPTY is stripped from the stored metadata, so an attached EMPTY view is indistinguishable from
    an ordinary one, and its "pretend we just refreshed" anchor exists only in the process that ran
    the CREATE.
    """
    name = "rmv_empty"
    node.query(f"DROP TABLE IF EXISTS {name}")
    node.query(
        f"CREATE MATERIALIZED VIEW {name} REFRESH EVERY 1 DAY (a DateTime, b UInt64) "
        f"ENGINE = MergeTree ORDER BY tuple() EMPTY "
        f"AS SELECT now() a, number b FROM numbers(2)"
    )
    try:
        wait_for_refresh_info(name, "status", lambda x: x == "Scheduled")
        assert refresh_info(name, "last_success_time") == "\\N"
        # Captured, not asserted, until after the restart: a refresh also writes the file, so
        # reading it afterwards could not tell a startup write from a stampede's write.
        before = [
            read_state_file(path) for path in find_refresh_state_files(DB_DISK_PATH)
        ]

        node.restart_clickhouse()

        wait_for_refresh_info(name, "status", lambda x: x == "Scheduled")
        # A lost schedule makes the view overdue by decades, so it refreshes within milliseconds of
        # the server accepting connections.
        time.sleep(3)
        assert refresh_info(name, "last_success_time") == "\\N"
        assert refresh_info(name, "last_refresh_time") == "\\N"
        assert profile_event("RefreshableViewRefreshSuccess") == 0

        assert len(before) == 1
        assert "last_completed_timeslot: 0\n" not in before[0]
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
        wait_for_refresh_info(name, "status", lambda x: x == "Disabled")
        assert refresh_info(name, "last_success_time") == "\\N"
        assert "refresh_state" in refresh_info(name, "exception")
        time.sleep(3)
        assert refresh_info(name, "status") == "Disabled"

        # Recoverable in place, which is what the operator-facing message promises. The schedule is
        # gone either way, so refreshing once here is the correct outcome.
        node.query(f"SYSTEM START VIEW {name}")
        wait_for_refresh_info(name, "last_success_time", lambda x: x not in ("", "\\N"))
        assert "refresh_state" not in refresh_info(name, "exception")

        # Recoverable without losing the table: removing the unreadable file restores the normal
        # first-start behaviour.
        node.stop_clickhouse()
        node.exec_in_container(["bash", "-c", f"rm -f {state_files[0]}"])
        node.start_clickhouse()
        wait_for_refresh_info(name, "last_success_time", lambda x: x not in ("", "\\N"))
    finally:
        node.query(f"DROP TABLE IF EXISTS {name}")


def test_refresh_is_not_published_when_the_state_cannot_be_persisted():
    name = "rmv_persist_fails"
    before = create_daily_rmv(name)
    state_files = find_refresh_state_files(DB_DISK_PATH)
    assert len(state_files) == 1
    # A directory where the temporary file goes makes the write fail with EISDIR for any uid,
    # unlike permission bits, which root would ignore.
    blocker = state_files[0] + ".tmp"
    failures_before = profile_event("RefreshableViewStatePersistFailed")

    try:
        node.exec_in_container(["bash", "-c", f"mkdir {blocker}"], user="root")
        try:
            node.query(f"SYSTEM REFRESH VIEW {name}")
            # Not SYSTEM WAIT VIEW: its predicate excludes Running and Scheduling, and the view
            # parks in Scheduling while the transition keeps failing.
            failures = node.query_with_retry(
                "SELECT sum(value) FROM system.events "
                "WHERE event = 'RefreshableViewStatePersistFailed'",
                check_callback=lambda x: int(x.strip()) > failures_before,
                retry_count=120,
            ).strip()
            assert int(failures) > failures_before
            assert refresh_info(name, "last_success_time") == before
            # Past one 5 s retry, so a transition that publishes late still reddens this.
            time.sleep(7)
            assert refresh_info(name, "last_success_time") == before
        finally:
            node.exec_in_container(["bash", "-c", f"rmdir {blocker}"], user="root")

        # Converges once the write can succeed, and only once: a retry storm would keep moving it.
        after = wait_for_refresh_info(
            name, "last_success_time", lambda x: x not in ("", "\\N", before)
        )
        time.sleep(7)
        assert refresh_info(name, "last_success_time") == after
        state_files = find_refresh_state_files(DB_DISK_PATH)
        assert len(state_files) == 1
        state = read_state_file(state_files[0])
        assert state.startswith("format version: 1\n")
        assert "last_success_time: 0\n" not in state
    finally:
        node.query(f"DROP TABLE IF EXISTS {name}")


def test_refresh_running_is_not_believed_after_a_crash():
    """A crash mid-refresh persists refresh_running, which the loader has to clear.

    Without that, the view starts up believing a refresh it cannot observe is in flight, and the
    reconciliation path that then cleans up ignores whether its own write succeeded.
    """
    name = "rmv_crash"
    node.query(f"DROP TABLE IF EXISTS {name}")
    node.query(
        f"CREATE MATERIALIZED VIEW {name} REFRESH EVERY 1 DAY (a DateTime, b UInt8) "
        f"ENGINE = MergeTree ORDER BY tuple() EMPTY "
        f"AS SELECT now() a, sleep(1) b FROM numbers(10) SETTINGS max_block_size = 1"
    )
    try:
        wait_for_refresh_info(name, "status", lambda x: x == "Scheduled")
        node.query(f"SYSTEM REFRESH VIEW {name}")
        node.query_with_retry(
            f"SELECT status FROM system.view_refreshes WHERE view = '{name}'",
            check_callback=lambda x: x.strip() == "Running",
            retry_count=300,
            sleep_time=0.1,
        )

        # The precondition the loader handles, made observable: without this the test would pass
        # either way, because the reconciliation path self-heals in one extra scheduling pass.
        state_files = find_refresh_state_files(DB_DISK_PATH)
        assert len(state_files) == 1
        persisted = read_state_file(state_files[0])
        assert "refresh_running: 1\n" in persisted, persisted

        leftover_warning = "znode says this replica is running refresh, but it isn't"
        warnings_before = count_in_server_logs(leftover_warning)
        node.stop_clickhouse(kill=True)
        node.start_clickhouse()

        wait_for_refresh_info(name, "status", lambda x: x == "Scheduled")
        assert count_in_server_logs(leftover_warning) == warnings_before
    finally:
        node.query(f"DROP TABLE IF EXISTS {name}")
