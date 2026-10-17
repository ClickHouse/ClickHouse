import os
import shlex
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# An uncoordinated refreshable view persists its schedule on the database disk. These tests read and
# edit that file, so they pin `database_disk` to a local disk at a known path, and opt out of
# with_remote_database_disk, which some builds enable by default and which would put the file in
# object storage instead.
# Every DROP here says SYNC: the assertions count state files across the whole disk, and a plain
# DROP returns before the background job has removed the dropped table's directory.
node = cluster.add_instance(
    "node",
    main_configs=["configs/database_disk.xml"],
    user_configs=["configs/settings.xml"],
    with_remote_database_disk=False,
    stay_alive=True,
)

DB_DISK_PATH = "/var/lib/clickhouse/disks/db_meta_disk"
OTHER_DB_DISK_PATH = "/var/lib/clickhouse/disks/other_db_disk"
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


def read_state_file(path):
    return node.exec_in_container(["bash", "-c", f"cat {shlex.quote(path)}"])


def refresh_info(name, column, database="default"):
    return node.query(
        f"SELECT {column} FROM system.view_refreshes "
        f"WHERE database = '{database}' AND view = '{name}'"
    ).strip()


def profile_event(name):
    return int(
        node.query(
            f"SELECT sum(value) FROM system.events WHERE event = '{name}'"
        ).strip()
    )


def wait_for_refresh_info(
    name, column, predicate, retry_count=120, sleep_time=0.5, database="default"
):
    """Poll one system.view_refreshes column until `predicate` holds, then assert that it does.

    query_with_retry returns its last result even when the callback never passed, so the assert is
    what makes this a check rather than a delay.
    """
    value = node.query_with_retry(
        f"SELECT {column} FROM system.view_refreshes "
        f"WHERE database = '{database}' AND view = '{name}'",
        check_callback=lambda x: predicate(x.strip()),
        retry_count=retry_count,
        sleep_time=sleep_time,
    ).strip()
    assert predicate(value), f"{database}.{name}.{column} is {value!r}"
    return value


def create_daily_rmv(name, database="default"):
    """A view that refreshes once now and then not again for a day.

    AFTER, not EVERY: EVERY 1 DAY without OFFSET is due at the next calendar midnight, so a run that
    straddles midnight would legitimately refresh again and read as a stampede. AFTER has no
    calendar boundary (RefreshSchedule::advance returns completion + the period).
    No EMPTY: the stored metadata does not keep the EMPTY flag, and an EMPTY view has no completed
    refresh anyway, while last_success_time is what these tests compare across a restart.
    """
    node.query(f"DROP TABLE IF EXISTS {database}.{name} SYNC")
    node.query(
        f"CREATE MATERIALIZED VIEW {database}.{name} REFRESH AFTER 1 DAY (a DateTime, b UInt64) "
        f"ENGINE = MergeTree ORDER BY tuple() AS SELECT now() a, number b FROM numbers(2)"
    )
    return wait_for_refresh_info(
        name, "last_success_time", lambda x: x not in ("", "\\N"), database=database
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
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_empty_view_keeps_its_schedule_across_restart():
    """An EMPTY view has no refresh to persist its schedule, so startup has to persist it.

    EMPTY is stripped from the stored metadata, so an attached EMPTY view is indistinguishable from
    an ordinary one, and its "pretend we just refreshed" anchor exists only in the process that ran
    the CREATE.
    """
    name = "rmv_empty"
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(
        f"CREATE MATERIALIZED VIEW {name} REFRESH AFTER 1 DAY (a DateTime, b UInt64) "
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
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


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
    try:
        # Inside the try: a leaked view keeps its state file, which would then fail every later
        # filesystem-wide count and report one fault as several.
        state_files = find_refresh_state_files(DB_DISK_PATH)
        assert len(state_files) == 1

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
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_finished_refresh_is_not_published_when_the_state_cannot_be_persisted():
    """Blocking the write while a refresh is in flight covers the transition that publishes it.

    The transition that records a finished refresh is what publishes last_completed_timeslot and the
    incremental cursor, so it is the only arm that can show the data landing while the schedule
    stays unpublished. It is also the only transition an uncoordinated view persists at all, which
    makes it the whole of the fail-closed contract.
    """
    name = "rmv_finish_persist_fails"
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(
        f"CREATE MATERIALIZED VIEW {name} REFRESH AFTER 1 DAY (a DateTime, b UInt8) "
        f"ENGINE = MergeTree ORDER BY tuple() "
        f"AS SELECT now() a, sleep(1) b FROM numbers(8) "
        f"SETTINGS max_block_size = 1, max_threads = 1"
    )
    try:
        before = wait_for_refresh_info(
            name, "last_success_time", lambda x: x not in ("", "\\N")
        )
        # now() is one value per refresh, and a non-APPEND refresh replaces the whole table, so this
        # moves if and only if the exchange happened.
        data_before = node.query(f"SELECT max(a) FROM {name}").strip()
        state_files = find_refresh_state_files(DB_DISK_PATH)
        assert len(state_files) == 1
        # A directory where the temporary file goes makes the write fail with EISDIR for any uid,
        # unlike permission bits, which root would ignore.
        blocker = state_files[0] + ".tmp"
        failures_before = profile_event("RefreshableViewStatePersistFailed")

        node.query(f"SYSTEM REFRESH VIEW {name}")
        wait_for_refresh_info(
            name, "status", lambda x: x == "Running", retry_count=300, sleep_time=0.1
        )
        try:
            node.exec_in_container(["bash", "-c", f"mkdir {blocker}"], user="root")
            # A refresh that finished before the blocker landed would have published already and
            # would leave this arm uncovered, so require that it is still running.
            assert refresh_info(name, "status") == "Running"

            failures = node.query_with_retry(
                "SELECT sum(value) FROM system.events "
                "WHERE event = 'RefreshableViewStatePersistFailed'",
                check_callback=lambda x: int(x.strip()) > failures_before,
                retry_count=120,
            ).strip()
            assert int(failures) > failures_before

            # The split this arm exists for: the refresh ran to completion and its data is live,
            # while the schedule it would advance was not published.
            data_blocked = node.query(f"SELECT max(a) FROM {name}").strip()
            assert data_blocked != data_before
            assert refresh_info(name, "last_success_time") == before
            # Past one 5 s retry, so a transition that publishes late still reddens this.
            time.sleep(7)
            assert node.query(f"SELECT max(a) FROM {name}").strip() == data_blocked
            assert refresh_info(name, "last_success_time") == before
        finally:
            node.exec_in_container(["bash", "-c", f"rmdir {blocker}"], user="root")

        # Converges once the write can succeed, and only once: a retry storm would keep moving it,
        # and a lost result would refresh the data again rather than publish the one already there.
        after = wait_for_refresh_info(
            name, "last_success_time", lambda x: x not in ("", "\\N", before)
        )
        assert node.query(f"SELECT max(a) FROM {name}").strip() == data_blocked
        time.sleep(7)
        assert refresh_info(name, "last_success_time") == after
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_a_failed_startup_save_is_retried_before_any_refresh():
    """startup() is the only chance an EMPTY view has to publish its anchor before it refreshes.

    Blocking the write needs the path before the view exists, so the UUID is given explicitly. A
    directory at the temporary name the file is written through fails the write with EISDIR for any
    uid, unlike permission bits, which root would ignore.
    """
    name = "rmv_startup_persist_fails"
    view_uuid = str(uuid.uuid4())
    server_uuid = node.query("SELECT serverUUID()").strip()
    state_dir = f"{DB_DISK_PATH}/store/{view_uuid[:3]}/{view_uuid}"
    blocker = f"{state_dir}/refresh_state.{server_uuid}.txt.tmp"
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    # The server has to be able to write here once the blocker is gone, whichever uid it runs as.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {blocker} && chown -R --reference={DB_DISK_PATH} {DB_DISK_PATH}/store",
        ],
        user="root",
    )
    try:
        failures_before = profile_event("RefreshableViewStatePersistFailed")
        successes_before = profile_event("RefreshableViewRefreshSuccess")
        # EMPTY is what makes the anchor worth keeping: it says the view was just refreshed, and
        # EVERY 2 SECOND is short enough that an anchor left in memory is refreshed past at once.
        node.query(
            f"CREATE MATERIALIZED VIEW {name} UUID '{view_uuid}' REFRESH EVERY 2 SECOND "
            f"(a DateTime) ENGINE = MergeTree ORDER BY tuple() EMPTY AS SELECT now() a"
        )
        failures = node.query_with_retry(
            "SELECT sum(value) FROM system.events "
            "WHERE event = 'RefreshableViewStatePersistFailed'",
            check_callback=lambda x: int(x.strip()) > failures_before,
            retry_count=120,
        ).strip()
        assert int(failures) > failures_before

        # Three timeslots and one 5 s retry interval, so a view that refreshes off the unpublished
        # anchor reddens this rather than merely being early.
        time.sleep(7)
        assert refresh_info(name, "last_success_time") == "\\N"
        assert profile_event("RefreshableViewRefreshSuccess") == successes_before
        assert find_refresh_state_files(state_dir) == []

        node.exec_in_container(["bash", "-c", f"rmdir {blocker}"], user="root")

        # Refreshing is held until the save succeeds, so a refresh happening at all is the retry.
        wait_for_refresh_info(name, "last_success_time", lambda x: x not in ("", "\\N"))
        assert find_refresh_state_files(state_dir) == [
            f"{state_dir}/refresh_state.{server_uuid}.txt"
        ]
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")
        node.exec_in_container(["bash", "-c", f"rm -rf {blocker}"], user="root")


def test_a_stopped_view_still_retries_its_failed_startup_save():
    """Stopping a view stops its refreshes, not its persistence.

    A view stopped before its anchor reached the disk is the shape of the reported stampede: the
    anchor never becomes durable, a restart reconstructs the epoch one instead, and the SYSTEM START
    VIEWS that follows refreshes every view at once.
    """
    name = "rmv_stopped_persist_fails"
    view_uuid = str(uuid.uuid4())
    server_uuid = node.query("SELECT serverUUID()").strip()
    state_dir = f"{DB_DISK_PATH}/store/{view_uuid[:3]}/{view_uuid}"
    blocker = f"{state_dir}/refresh_state.{server_uuid}.txt.tmp"
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {blocker} && chown -R --reference={DB_DISK_PATH} {DB_DISK_PATH}/store",
        ],
        user="root",
    )
    try:
        failures_before = profile_event("RefreshableViewStatePersistFailed")
        # EVERY 1 DAY, so nothing here depends on when refreshes resume.
        node.query(
            f"CREATE MATERIALIZED VIEW {name} UUID '{view_uuid}' REFRESH EVERY 1 DAY "
            f"(a DateTime) ENGINE = MergeTree ORDER BY tuple() EMPTY AS SELECT now() a"
        )
        node.query(f"SYSTEM STOP VIEW {name}")
        failures = node.query_with_retry(
            "SELECT sum(value) FROM system.events "
            "WHERE event = 'RefreshableViewStatePersistFailed'",
            check_callback=lambda x: int(x.strip()) > failures_before,
            retry_count=120,
        ).strip()
        assert int(failures) > failures_before
        assert find_refresh_state_files(state_dir) == []

        node.exec_in_container(["bash", "-c", f"rmdir {blocker}"], user="root")

        state_path = f"{state_dir}/refresh_state.{server_uuid}.txt"
        for _ in range(120):
            if find_refresh_state_files(state_dir) == [state_path]:
                break
            time.sleep(0.5)
        assert find_refresh_state_files(state_dir) == [state_path]
        # The file appears while the scheduling pass still holds the view in Scheduling, so the
        # status it settles back to is what says the retry started no refresh.
        wait_for_refresh_info(name, "status", lambda x: x == "Disabled")
        assert refresh_info(name, "last_success_time") == "\\N"
        assert "last_completed_timeslot: 0\n" not in read_state_file(state_path)
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")
        node.exec_in_container(["bash", "-c", f"rm -rf {blocker}"], user="root")


def create_slow_rmv(name):
    """A view whose refresh takes seconds, so a control command can be issued while it runs."""
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(
        f"CREATE MATERIALIZED VIEW {name} REFRESH AFTER 1 DAY (a DateTime, b UInt8) "
        f"ENGINE = MergeTree ORDER BY tuple() "
        f"AS SELECT now() a, sleep(1) b FROM numbers(3) "
        f"SETTINGS max_block_size = 1, max_threads = 1"
    )
    return wait_for_refresh_info(
        name, "last_success_time", lambda x: x not in ("", "\\N")
    )


def test_a_starting_refresh_is_not_persisted():
    """Only transitions that do not start a refresh are persisted.

    A starting refresh publishes nothing a restart cannot recompute from the last persisted
    finish, and persisting it would release the task's mutex mid-start. Skipping it on the write
    side is what makes `refresh_running: 0` an invariant of every payload ever written, because
    the loader honours whatever the file says. This asserts it at the one moment the in-memory
    value differs.
    """
    name = "rmv_starting"
    create_slow_rmv(name)
    try:
        state_files = find_refresh_state_files(DB_DISK_PATH)
        assert len(state_files) == 1

        node.query(f"SYSTEM REFRESH VIEW {name}")
        wait_for_refresh_info(
            name, "status", lambda x: x == "Running", retry_count=300, sleep_time=0.1
        )
        # Read while the refresh is in flight, which is the only window in which the in-memory
        # root_znode says refresh_running = true.
        assert refresh_info(name, "status") == "Running"
        persisted = read_state_file(state_files[0])
        assert "refresh_running: 0\n" in persisted, persisted

        node.query(f"SYSTEM WAIT VIEW {name}")
        # The skip is confined to the start transition, so the finished one does publish: the payload
        # has to change. last_completed_timeslot alone guarantees it moves, since AFTER anchors on the
        # end time and the refresh sleeps for seconds.
        published = read_state_file(state_files[0])
        assert published != persisted
        assert "refresh_running: 0\n" in published, published
        assert find_refresh_state_files(DB_DISK_PATH) == state_files
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_state_follows_the_owning_databases_disk():
    """A database can keep its metadata off the server-global database_disk, and this follows it.

    {global database_disk read-only or simply elsewhere} plus {database created with SETTINGS disk}
    is a supported server on which DDL works, so resolving the disk globally would silently leave the
    stampede in place for every view in such a database.
    """
    database = "rmv_other_disk_db"
    name = "rmv_on_other_disk"
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(
        f"CREATE DATABASE {database} ENGINE = Atomic SETTINGS disk = 'other_db_disk'"
    )
    try:
        before = create_daily_rmv(name, database=database)

        state_files = find_refresh_state_files(OTHER_DB_DISK_PATH)
        assert len(state_files) == 1
        server_uuid = node.query("SELECT serverUUID()").strip()
        assert os.path.basename(state_files[0]) == f"refresh_state.{server_uuid}.txt"
        assert find_refresh_state_files(DB_DISK_PATH) == []

        node.restart_clickhouse()

        wait_for_refresh_info(
            name, "status", lambda x: x == "Scheduled", database=database
        )
        time.sleep(3)
        assert refresh_info(name, "last_success_time", database=database) == before

        # The state file has no removal hook of its own: it is removed with the store directory, by
        # dropTableFinally, which DROP TABLE ... SYNC waits for. So this cannot race.
        node.query(f"DROP TABLE {database}.{name} SYNC")
        assert find_refresh_state_files(OTHER_DB_DISK_PATH) == []
    finally:
        node.query(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_moving_a_view_to_another_database_keeps_its_schedule():
    """The state file is named after the view's own UUID, which outlives a rename.

    Only a view without an inner table can move between databases, and only between databases on one
    disk: DatabaseAtomic moves the metadata file with the source database's disk, so a destination on
    another disk fails. A database's `disk` is also not alterable. Between them, the disk resolved
    when the view started cannot become the wrong one.
    """
    same_disk_db = "rmv_move_db"
    other_disk_db = "rmv_move_other_disk_db"
    name = "rmv_moved"
    target = "rmv_moved_target"
    node.query(f"DROP DATABASE IF EXISTS {same_disk_db} SYNC")
    node.query(f"DROP DATABASE IF EXISTS {other_disk_db} SYNC")
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(f"DROP TABLE IF EXISTS {target} SYNC")
    node.query(f"CREATE DATABASE {same_disk_db} ENGINE = Atomic")
    node.query(
        f"CREATE DATABASE {other_disk_db} ENGINE = Atomic SETTINGS disk = 'other_db_disk'"
    )
    try:
        node.query(
            f"CREATE TABLE {target} (a DateTime, b UInt64) ENGINE = MergeTree ORDER BY tuple()"
        )
        # TO, so there is no inner table and the view is movable at all.
        node.query(
            f"CREATE MATERIALIZED VIEW {name} REFRESH AFTER 1 DAY TO {target} "
            f"AS SELECT now() a, number b FROM numbers(2)"
        )
        before = wait_for_refresh_info(
            name, "last_success_time", lambda x: x not in ("", "\\N")
        )
        state_files = find_refresh_state_files(DB_DISK_PATH)
        assert len(state_files) == 1

        node.query(f"RENAME TABLE {name} TO {same_disk_db}.{name}")
        assert find_refresh_state_files(DB_DISK_PATH) == state_files

        # The out-of-range arm of the same statement: one variable changes, the destination's disk.
        assert (
            node.query_and_get_error(
                f"RENAME TABLE {same_disk_db}.{name} TO {other_disk_db}.{name}"
            )
            != ""
        )
        assert (
            node.query(
                f"SELECT database FROM system.tables WHERE name = '{name}'"
            ).strip()
            == same_disk_db
        )
        assert find_refresh_state_files(OTHER_DB_DISK_PATH) == []

        node.restart_clickhouse()

        wait_for_refresh_info(
            name, "status", lambda x: x == "Scheduled", database=same_disk_db
        )
        time.sleep(3)
        assert refresh_info(name, "last_success_time", database=same_disk_db) == before
    finally:
        node.query(f"DROP DATABASE IF EXISTS {same_disk_db} SYNC")
        node.query(f"DROP DATABASE IF EXISTS {other_disk_db} SYNC")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")
        node.query(f"DROP TABLE IF EXISTS {target} SYNC")


def test_a_view_migrated_from_an_ordinary_database_keeps_its_schedule():
    """Tables in an Ordinary database have no UUID, and the state file is named after one.

    So the migration to Atomic is the point at which the schedule becomes persistable at all, and the
    view is already running by then. APPEND, because a non-APPEND refreshable view cannot be created
    in an Ordinary database (Code: 80).
    """
    ordinary_db = "rmv_ordinary_db"
    name = "rmv_migrated"
    node.query(f"DROP DATABASE IF EXISTS {ordinary_db} SYNC")
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(
        f"CREATE DATABASE {ordinary_db} ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    try:
        node.query(
            f"CREATE MATERIALIZED VIEW {ordinary_db}.{name} REFRESH AFTER 1 DAY APPEND "
            f"(a DateTime, b UInt64) ENGINE = MergeTree ORDER BY tuple() "
            f"AS SELECT now() a, number b FROM numbers(2)"
        )
        wait_for_refresh_info(
            name,
            "last_success_time",
            lambda x: x not in ("", "\\N"),
            database=ordinary_db,
        )
        assert find_refresh_state_files(DB_DISK_PATH) == []

        node.query(f"RENAME TABLE {ordinary_db}.{name} TO default.{name}")
        before = refresh_info(name, "last_success_time")
        rows = node.query(f"SELECT count() FROM {name}").strip()

        # The rename only marks the save pending; the scheduling task performs it.
        state_files = []
        for _ in range(120):
            state_files = find_refresh_state_files(DB_DISK_PATH)
            if state_files:
                break
            time.sleep(0.5)
        assert len(state_files) == 1
        view_uuid = node.query(
            f"SELECT uuid FROM system.tables WHERE database = 'default' AND name = '{name}'"
        ).strip()
        assert view_uuid != str(uuid.UUID(int=0))
        assert view_uuid in state_files[0]

        node.restart_clickhouse()

        wait_for_refresh_info(name, "status", lambda x: x == "Scheduled")
        time.sleep(3)
        # With no file the moved view would refresh a day early and append its source a second time.
        assert refresh_info(name, "last_success_time") == before
        assert node.query(f"SELECT count() FROM {name}").strip() == rows
    finally:
        node.query(f"DROP DATABASE IF EXISTS {ordinary_db} SYNC")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")
