import subprocess
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/query_log.xml",
        "configs/startup_scripts.xml",
    ],
    macros={"replica": "node", "shard": "node"},
    with_zookeeper=True,
    stay_alive=True,
)
good = cluster.add_instance(
    "good",
    main_configs=["configs/good_script.xml"],
    stay_alive=True,
)
bad = cluster.add_instance(
    "bad",
    main_configs=["configs/bad_script.xml"],
    stay_alive=True,
)


def _built_with_thread_sanitizer():
    # `ClickHouseInstance.is_built_with_thread_sanitizer()` needs a running server, which is too
    # late: `cluster.start()` starts every registered instance before any test body runs, so the
    # in-test skip cannot avoid the cost of this instance. Ask the binary directly instead.
    # Unknown answers count as ThreadSanitizer: skipping is a silent loss of the only coverage
    # this race has, while a needless instance is only slower.
    # `cluster.server_bin_path` is the binary the instances will run. Re-deriving it here would
    # diverge from it, because the fallback used when the environment does not name one searches
    # `PATH` before `/usr/bin`.
    try:
        flags = subprocess.run(
            [
                cluster.server_bin_path,
                "local",
                "--query",
                "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS'",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=True,
        ).stdout
    except Exception:
        return True
    return "-fsanitize=thread" in flags


# Registering this instance costs a whole extra server plus a non-lazy dictionary load on every run
# of this file, and only a ThreadSanitizer build can observe the race it reproduces.
profiler_race = (
    cluster.add_instance(
        "profiler_race",
        main_configs=[
            "configs/profiler_race_script.xml",
            "configs/profiler_race_logs.xml",
        ],
        user_configs=["configs/profiler_race_users.xml"],
        stay_alive=True,
    )
    if _built_with_thread_sanitizer()
    else None
)

# Values of the StartupScriptsExecutionState metric.
STATE_SUCCESS = 1
STATE_FAILURE = 2

# How many times the profiler-race instance is restarted. Each restart is one chance for the
# profiler signal to land while the loader thread publishes the new per-user Counters. One restart
# samples that window thousands of times and reports the race on its own, so the repetitions are
# margin only and a handful of them keeps the cost of a sanitizer run bounded.
PROFILER_RACE_RESTARTS = 6


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_execution_state(instance):
    return int(
        instance.query(
            "SELECT value FROM system.metrics WHERE metric = 'StartupScriptsExecutionState'"
        ).strip()
    )


def test_startup_scripts(start_cluster):
    tables = node.query("SHOW TABLES")
    assert "TestTable" in tables
    assert "test_dict" in tables
    assert (
        node.query(
            "SELECT value, changed FROM system.settings WHERE name = 'skip_unavailable_shards'"
        )
        == "0\t0\n"
    )

    tables = node.query("SHOW TABLES FROM replicated")
    assert "test_replica" in tables


def test_startup_execution_state(start_cluster):
    """
    Making sure that the StartupScriptsExecutionState metric is set correctly
    and that the dimensional metric startup_scripts_failure_reason is recorded.
    """

    def assert_startup_script_failed():
        assert get_execution_state(bad) == STATE_FAILURE

    assert get_execution_state(good) == STATE_SUCCESS
    assert_startup_script_failed()

    assert bool(
        good.query(
            """
            SELECT count() = 0 FROM system.dimensional_metrics
            WHERE metric = 'startup_scripts_failure_reason'
            """
        ).strip()
    )

    bad.stop_clickhouse()
    # Set throw_on_error: true for the startup_script
    bad.replace_in_config(
        "/etc/clickhouse-server/config.d/bad_script.xml",
        "<throw_on_error>false",
        "<throw_on_error>true",
    )
    bad.start_clickhouse(start_wait_sec=120, expected_to_fail=True)
    # server can't start with errors in startup_script
    assert bad.get_process_pid("clickhouse") is None
    assert bad.contains_in_log("Failed to parse startup scripts file")
    # Logs contains the original error
    assert bad.contains_in_log(
        "Unknown table expression identifier 'non_existent_table'"
    )
    assert bad.contains_in_log("Cannot finish startup_script successfully")

    bad.replace_in_config(
        "/etc/clickhouse-server/config.d/bad_script.xml",
        "<throw_on_error>true",
        "<throw_on_error>false",
    )
    bad.start_clickhouse()
    assert bad.get_process_pid("clickhouse") is not None

    # startup script wasn't executed, but the server is up
    assert_startup_script_failed()

    assert (
        int(
            bad.query(
                """
                SELECT value
                FROM system.dimensional_metrics
                WHERE 1
                    AND metric = 'startup_scripts_failure_reason'
                    AND labels['error_name'] = 'UNKNOWN_TABLE'
                """
            ).strip()
        )
        == 1
    )


def test_reload_config_does_not_rerun_startup_scripts(start_cluster):
    # Startup scripts contain non-idempotent queries.
    # So if SYSTEM RELOAD CONFIG invoked startup scripts, the metric would turn red.
    assert get_execution_state(node) == STATE_SUCCESS
    node.query("SYSTEM RELOAD CONFIG")
    assert get_execution_state(node) == STATE_SUCCESS


def restart_profiler_race_instance():
    # `SETTINGS(dictionary_lazy_load = 0)` is stored in the dictionary metadata, so a surviving
    # dictionary is loaded again while its table is attached, before the startup script runs. That
    # earlier load creates the source user's ProcessListForUser, leaving the startup script with the
    # existing-user branch, which is not the racing scenario. Dropping it makes the startup script
    # the only creator.
    profiler_race.query("DROP DICTIONARY IF EXISTS profiler_race_dict")

    # The thread log survives the restart, so return the server's own clock reading first: it scopes
    # the queries below to the run that is about to start. Python's clock is not interchangeable here
    # because the container may be skewed against the host.
    before = profiler_race.query("SELECT toString(now64(6))").strip()

    # Stop by pid and wait for the process to be gone: the shared `stop_clickhouse()` helper was
    # observed to skip the stop here, silently turning every restart into a no-op. Restarting
    # explicitly keeps each iteration a real startup, which is what this test measures.
    pid = profiler_race.get_process_pid("clickhouse")
    assert pid is not None, "server is not running, cannot restart it"
    profiler_race.exec_in_container(
        ["bash", "-c", f"kill -9 {pid}"], user="root", nothrow=True
    )
    for _ in range(120):
        if profiler_race.get_process_pid("clickhouse") is None:
            break
        time.sleep(0.5)
    assert profiler_race.get_process_pid("clickhouse") is None, "server did not stop"
    profiler_race.start_clickhouse(start_wait_sec=180)
    return before


def test_profiler_vs_processlist_publication(start_cluster):
    # The instance is only registered on ThreadSanitizer. The server-side check still runs when it
    # is: it is the authoritative one, and it covers the case where the pre-start probe above had to
    # guess.
    if profiler_race is None or not profiler_race.is_built_with_thread_sanitizer():
        pytest.skip("the race is only observable under ThreadSanitizer")

    def dictionary_source_logins(current_run_only):
        # `rotateOnOpen` is set for these instances, so every start moves the previous log aside and
        # the un-globbed file covers exactly the current run. The glob adds the gzipped rotations of
        # earlier runs, which is what the cumulative check at the end of the test wants.
        path = "/var/log/clickhouse-server/clickhouse-server.log"
        if not current_run_only:
            path += "*"
        return int(
            profiler_race.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"zcat -f {path} "
                    "| grep -ac \"Authenticating user 'dict_source_user'\" || true",
                ],
                user="root",
            ).strip()
            or 0
        )

    def startup_thread_row(since):
        # Neither `system.events` nor `system.query_log` can answer this: the former is process-wide
        # and also fed by the always-on global profiler, the latter reports the thread GROUP
        # aggregate, and the dictionary loader thread joins that same group with its own profiler.
        # The race needs the thread running `loadStartupScripts` as its reader, so count the samples
        # that thread booked itself: `query_thread_log` reports each thread's own counters, and the
        # startup thread is the group's master thread. It is not renamed, so its thread_name is
        # `Unknown` and cannot be used to identify it.
        profiler_race.query("SYSTEM FLUSH LOGS")
        row = profiler_race.query(
            "SELECT thread_id, ProfileEvents['QueryProfilerRuns'] "
            "FROM system.query_thread_log "
            "WHERE query LIKE '%profiler_race_dict%' AND thread_id = master_thread_id "
            # This query is logged too and mentions the dictionary; the table name it reads is
            # what tells its own rows apart from the startup script's.
            "AND query NOT LIKE '%query_thread_log%' "
            # The log is a persistent table and the restarts do not wipe the data directory, so
            # without this the newest row can belong to an earlier restart that did pass.
            f"AND event_time_microseconds > toDateTime64('{since}', 6) "
            "ORDER BY event_time_microseconds DESC LIMIT 1"
        ).split()
        assert row, (
            "the startup thread logged no row for the dictionary query in this run, so neither "
            "assertion below can be evaluated"
        )
        return int(row[0]), int(row[1])

    def loader_threads_sharing_group(startup_tid, since):
        # A thread that joined the startup thread's group reports that thread as its master; a loader
        # that failed to inherit the group and built its own reports itself instead. Only in the
        # former case does `ProcessList::insert` publish the new per-user Counters into the chain the
        # startup thread walks, which is the whole point of this test. The loader's own row carries
        # the source SELECT rather than the CREATE DICTIONARY, because `ProcessList::insert`
        # overwrites the query it inherited at attach.
        return int(
            profiler_race.query(
                "SELECT count() FROM system.query_thread_log "
                f"WHERE master_thread_id = {startup_tid} AND thread_id != master_thread_id "
                "AND thread_name = 'ExternalLoader' "
                f"AND event_time_microseconds > toDateTime64('{since}', 6)"
            ).strip()
            or 0
        )

    for _ in range(PROFILER_RACE_RESTARTS):
        since = restart_profiler_race_instance()
        assert get_execution_state(profiler_race) == STATE_SUCCESS
        # The profiler is armed per thread on a best-effort basis: `initQueryProfiler` returns early
        # when there is no trace collector and swallows timer-creation failures. Without a positive
        # oracle a silently disarmed startup thread would never enter the racing window, yet the
        # absence of a TSan report would still read as a pass. A healthy startup samples thousands
        # of times.
        startup_tid, samples = startup_thread_row(since)
        assert samples > 1000, (
            f"the query profiler sampled the startup thread only {samples} times, so the racing "
            "window was not meaningfully exercised and the absence of a TSan report proves nothing"
        )
        # The assertion above covers the reader side only. The group is handed to the loader thread
        # on a best-effort basis too: `ThreadGroupSwitcher` swallows attach failures and nulls its
        # group, after which the dictionary source opens its own and publishes out of the startup
        # thread's reach. Every other assertion here still passes in that state.
        loaders = loader_threads_sharing_group(startup_tid, since)
        assert loaders > 0, (
            "the dictionary loader did not join the startup thread's group, so the per-user "
            "Counters were published into a chain that thread never walks and the cross-thread "
            "publication this test targets was not exercised"
        )
        # Only the FIRST insert for a user constructs the ProfileEvents::Counters this test races
        # against; later ones find the entry and publish an address that has long been visible. A
        # dictionary left over from the previous run is loaded once more at attach time, ahead of the
        # startup script, which would silently reduce the script to that second case.
        logins = dictionary_source_logins(current_run_only=True)
        assert logins == 1, (
            f"the dictionary source user logged in {logins} times in this run instead of once, so "
            "something loaded the dictionary before the startup script did and the first-time "
            "per-user Counters construction was not exercised on the startup path"
        )

    # Log rotation keeps a bounded number of files, so this cannot be compared against the restart
    # count directly. What matters is that the startup script really ran on the restarts that are
    # still on disk.
    assert dictionary_source_logins(current_run_only=False) > 0, (
        "the dictionary source query never ran, so the startup path this test is meant to "
        "exercise was not exercised at all"
    )

    # The sanitizer writes to stderr, which the logger config redirects to stderr.log. The error log
    # is grepped too as a cheap fallback in case that redirection is not in place.
    for filename in ("stderr.log", "clickhouse-server.err.log"):
        report = profiler_race.grep_in_log(
            "ThreadSanitizer: data race", from_host=True, filename=filename, after=40
        )
        assert not report, f"ThreadSanitizer report in {filename}:\n{report}"
