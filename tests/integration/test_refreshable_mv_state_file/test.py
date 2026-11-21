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
    main_configs=["configs/database_disk.xml", "configs/remote_servers.xml"],
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
        f"AS SELECT now() a, sleep(1) b FROM numbers(6) "
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
    renamed = "rmv_starting_renamed"
    node.query(f"DROP TABLE IF EXISTS {renamed} SYNC")
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

        # A rename re-saves the state under the view's (here unchanged) UUID; mid-refresh it must
        # not save the running one.
        node.query(f"RENAME TABLE {name} TO {renamed}")
        time.sleep(1)
        renamed_state = read_state_file(state_files[0])
        assert refresh_info(renamed, "status") == "Running"
        assert "refresh_running: 0\n" in renamed_state, renamed_state
        name = renamed

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
        node.query(f"DROP TABLE IF EXISTS {renamed} SYNC")


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


def test_a_target_that_keeps_data_only_in_memory_is_refilled_after_restart():
    """Such a target is empty after a restart, so a restored schedule would leave it empty until the
    next refresh, up to a day later. These views refresh at startup instead and persist nothing.
    """
    memory_view = "rmv_memory_target"
    join_target = "rmv_join_target"
    join_view = "rmv_join_view"
    append_target = "rmv_append_memory_target"
    append_view = "rmv_append_memory_view"
    alias_target = "rmv_alias_memory_target"
    alias_view = "rmv_alias_view"
    buffer_target = "rmv_buffer_target"
    buffer_view = "rmv_buffer_view"
    buffer_destination = "rmv_buffer_destination"
    destination_buffer = "rmv_destination_buffer"
    destination_buffer_view = "rmv_destination_buffer_view"
    forwarding_source = "rmv_forwarding_source"
    forwarding_target = "rmv_forwarding_memory_target"
    forwarding_view = "rmv_forwarding_view"
    fanout_source = "rmv_fanout_source"
    fanout_target = "rmv_fanout_memory_target"
    fanout_view = "rmv_fanout_view"
    durable_fanout_source = "rmv_durable_fanout_source"
    durable_fanout_target = "rmv_durable_fanout_target"
    durable_fanout_view = "rmv_durable_fanout_view"
    distributed_target = "rmv_distributed_memory_target"
    distributed = "rmv_local_distributed"
    distributed_view = "rmv_distributed_view"
    remote_distributed_target = "rmv_remote_distributed_target"
    remote_distributed = "rmv_remote_distributed"
    remote_distributed_view = "rmv_remote_distributed_view"
    time_series = "rmv_time_series"
    time_series_view = "rmv_time_series_view"
    views = [
        memory_view,
        join_view,
        append_view,
        alias_view,
        buffer_view,
        destination_buffer_view,
        forwarding_view,
        fanout_view,
        distributed_view,
        time_series_view,
    ]
    durable_views = [durable_fanout_view, remote_distributed_view]
    tables = views + [
        durable_fanout_view,
        remote_distributed_view,
        join_target,
        append_target,
        alias_target,
        buffer_target,
        destination_buffer,
        buffer_destination,
        forwarding_target,
        forwarding_source,
        fanout_target,
        fanout_source,
        durable_fanout_target,
        durable_fanout_source,
        distributed,
        distributed_target,
        remote_distributed,
        remote_distributed_target,
        time_series,
    ]
    for table in tables:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    try:
        node.query(
            f"CREATE MATERIALIZED VIEW {memory_view} REFRESH AFTER 1 DAY (b UInt64) "
            f"ENGINE = Memory AS SELECT number b FROM numbers(2)"
        )
        node.query(
            f"CREATE TABLE {join_target} (b UInt64, c UInt64) "
            f"ENGINE = Join(ANY, LEFT, b) SETTINGS persistent = 0"
        )
        node.query(
            f"CREATE MATERIALIZED VIEW {join_view} REFRESH AFTER 1 DAY TO {join_target} "
            f"AS SELECT number b, number * 10 c FROM numbers(2)"
        )
        node.query(f"CREATE TABLE {append_target} (b UInt64) ENGINE = Memory")
        node.query(
            f"CREATE MATERIALIZED VIEW {append_view} REFRESH AFTER 1 DAY APPEND "
            f"TO {append_target} AS SELECT number b FROM numbers(2)"
        )
        # An Alias stores nothing itself: its Memory target is what the restart empties.
        node.query(f"CREATE TABLE {alias_target} ENGINE = Alias({append_target})")
        node.query(
            f"CREATE MATERIALIZED VIEW {alias_view} REFRESH AFTER 1 DAY APPEND "
            f"TO {alias_target} AS SELECT number + 10 b FROM numbers(2)"
        )
        # No destination, and thresholds the test never reaches: only the shutdown flush clears it.
        node.query(
            f"CREATE TABLE {buffer_target} (b UInt64) "
            f"ENGINE = Buffer('', '', 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000)"
        )
        node.query(
            f"CREATE MATERIALIZED VIEW {buffer_view} REFRESH AFTER 1 DAY APPEND "
            f"TO {buffer_target} AS SELECT number b FROM numbers(2)"
        )
        # A destination does not help: rows still buffered at a crash are lost, and a restored
        # APPEND INCREMENTAL cursor would skip them for good.
        node.query(
            f"CREATE TABLE {buffer_destination} (b UInt64) ENGINE = MergeTree ORDER BY b"
        )
        node.query(
            f"CREATE TABLE {destination_buffer} (b UInt64) ENGINE = Buffer(currentDatabase(), "
            f"{buffer_destination}, 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000)"
        )
        node.query(
            f"CREATE MATERIALIZED VIEW {destination_buffer_view} REFRESH AFTER 1 DAY APPEND "
            f"TO {destination_buffer} AS SELECT number b FROM numbers(2)"
        )
        # An insert into a materialized view lands in its target, here a Memory table.
        node.query(f"CREATE TABLE {forwarding_source} (b UInt64) ENGINE = Null")
        node.query(
            f"CREATE MATERIALIZED VIEW {forwarding_target} ENGINE = Memory "
            f"AS SELECT b FROM {forwarding_source}"
        )
        node.query(
            f"CREATE MATERIALIZED VIEW {forwarding_view} REFRESH AFTER 1 DAY APPEND "
            f"TO {forwarding_target} AS SELECT number b FROM numbers(2)"
        )
        # The rows also reach the materialized views over the target, here into Memory.
        node.query(f"CREATE TABLE {fanout_source} (b UInt64) ENGINE = Null")
        node.query(
            f"CREATE MATERIALIZED VIEW {fanout_target} ENGINE = Memory "
            f"AS SELECT b FROM {fanout_source}"
        )
        node.query(
            f"CREATE MATERIALIZED VIEW {fanout_view} REFRESH AFTER 1 DAY APPEND "
            f"TO {fanout_source} AS SELECT number b FROM numbers(2)"
        )
        # Control: the same pipeline into MergeTree keeps its schedule.
        node.query(f"CREATE TABLE {durable_fanout_source} (b UInt64) ENGINE = Null")
        node.query(
            f"CREATE MATERIALIZED VIEW {durable_fanout_target} ENGINE = MergeTree "
            f"ORDER BY b AS SELECT b FROM {durable_fanout_source}"
        )
        node.query(
            f"CREATE MATERIALIZED VIEW {durable_fanout_view} REFRESH AFTER 1 DAY APPEND "
            f"TO {durable_fanout_source} AS SELECT number b FROM numbers(2)"
        )
        # A Distributed table writes the rows of a local shard straight into its table, here Memory.
        node.query(f"CREATE TABLE {distributed_target} (b UInt64) ENGINE = Memory")
        node.query(
            f"CREATE TABLE {distributed} (b UInt64) "
            f"ENGINE = Distributed(local_shard, currentDatabase(), {distributed_target})"
        )
        node.query(
            f"CREATE MATERIALIZED VIEW {distributed_view} REFRESH AFTER 1 DAY APPEND "
            f"TO {distributed} AS SELECT number b FROM numbers(2)"
        )
        # Control: a Distributed table with no local shard keeps its schedule.
        node.query(
            f"CREATE TABLE {remote_distributed_target} (b UInt64) ENGINE = MergeTree ORDER BY b"
        )
        node.query(
            f"CREATE TABLE {remote_distributed} (b UInt64) "
            f"ENGINE = Distributed(remote_shard, currentDatabase(), {remote_distributed_target})"
        )
        node.query(
            f"CREATE MATERIALIZED VIEW {remote_distributed_view} REFRESH AFTER 1 DAY APPEND "
            f"TO {remote_distributed} AS SELECT number b FROM numbers(2)"
        )
        # A TimeSeries table writes the rows into its inner tables, here Memory.
        node.query(
            f"CREATE TABLE {time_series} ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0 "
            f"SAMPLES INNER ENGINE = Memory TAGS INNER ENGINE = Memory "
            f"TIME RANGES INNER ENGINE = Memory METRIC FAMILIES INNER ENGINE = Memory",
            settings={"enable_time_series_table": 1},
        )
        node.query(
            f"CREATE MATERIALIZED VIEW {time_series_view} REFRESH AFTER 1 DAY APPEND "
            f"TO {time_series} AS SELECT 'metric' AS metric_name, "
            f"map('instance', toString(number)) AS tags, "
            f"[(toDateTime64('2026-10-23 00:00:00', 3), toFloat64(number))] AS samples "
            f"FROM numbers(2)"
        )
        before = {
            view: wait_for_refresh_info(
                view, "last_success_time", lambda x: x not in ("", "\\N")
            )
            for view in views + durable_views
        }
        durable_uuids = [
            node.query(
                f"SELECT uuid FROM system.tables "
                f"WHERE database = 'default' AND name = '{view}'"
            ).strip()
            for view in durable_views
        ]

        node.restart_clickhouse()

        for view in views:
            wait_for_refresh_info(
                view,
                "last_success_time",
                lambda x, view=view: x not in ("", "\\N", before[view]),
            )
        assert node.query(f"SELECT count() FROM {memory_view}").strip() == "2"
        assert (
            node.query(f"SELECT joinGet('{join_target}', 'c', toUInt64(1))").strip()
            == "10"
        )
        # One startup refresh of each view into the emptied target, not a skipped one and not two.
        assert (
            node.query(
                f"SELECT countIf(b < 10), countIf(b >= 10) FROM {append_target}"
            ).strip()
            == "2\t2"
        )
        assert node.query(f"SELECT count() FROM {buffer_target}").strip() == "2"
        assert node.query(f"SELECT count() FROM {forwarding_target}").strip() == "2"
        assert node.query(f"SELECT count() FROM {fanout_target}").strip() == "2"
        assert node.query(f"SELECT count() FROM {distributed_target}").strip() == "2"
        assert (
            node.query(f"SELECT count() FROM timeSeriesSamples({time_series})").strip()
            == "2"
        )
        for view in durable_views:
            assert refresh_info(view, "last_success_time") == before[view]
        assert node.query(f"SELECT count() FROM {durable_fanout_target}").strip() == "2"
        node.query(f"SYSTEM FLUSH DISTRIBUTED {remote_distributed}")
        assert (
            node.query(f"SELECT count() FROM {remote_distributed_target}").strip()
            == "2"
        )
        state_files = find_refresh_state_files(DB_DISK_PATH)
        assert len(state_files) == len(durable_uuids)
        for view_uuid in durable_uuids:
            assert any(view_uuid in path for path in state_files)
    finally:
        for table in tables:
            node.query(f"DROP TABLE IF EXISTS {table} SYNC")
