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
profiler_race = cluster.add_instance(
    "profiler_race",
    main_configs=["configs/profiler_race_script.xml"],
    user_configs=["configs/profiler_race_users.xml"],
    stay_alive=True,
)

# Values of the StartupScriptsExecutionState metric.
STATE_SUCCESS = 1
STATE_FAILURE = 2

# How many times the profiler-race instance is restarted. Each restart is one chance for the
# profiler signal to land while the loader thread publishes the new per-user Counters.
PROFILER_RACE_RESTARTS = 12


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
    # `stop_clickhouse()` finds the process with `ps -C clickhouse`, which matches on `comm`. The
    # server re-execs itself for `--daemon`, so its `comm` is `exe` and `ps -C clickhouse` finds
    # nothing: the helper would log "already stopped" and skip the restart, leaving the test
    # asserting nothing. Stop it by pid instead, then wait for the process to be gone.
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


def test_profiler_vs_processlist_publication(start_cluster):
    # Guard against the test silently degenerating into a no-op: every iteration must actually run
    # the startup script, which is what puts the loader thread and the profiled startup thread on
    # the same Counters chain.
    def dictionary_source_logins():
        # The server rotates its log on open, so each restart leaves a compressed
        # clickhouse-server.log.N.gz. Count over all of them (zcat -f handles both the plain
        # current log and the gzipped rotations).
        return int(
            profiler_race.exec_in_container(
                [
                    "bash",
                    "-c",
                    "zcat -f /var/log/clickhouse-server/clickhouse-server.log* "
                    "| grep -ac \"Authenticating user 'dict_source_user'\" || true",
                ],
                user="root",
            ).strip()
            or 0
        )

    for _ in range(PROFILER_RACE_RESTARTS):
        restart_profiler_race_instance()
        assert get_execution_state(profiler_race) == STATE_SUCCESS

    # Log rotation keeps a bounded number of files, so this cannot be compared against the restart
    # count directly. What matters is that the startup script really ran on the restarts that are
    # still on disk: if it had silently stopped running, this would be 0 and the test would be
    # asserting nothing.
    assert dictionary_source_logins() > 0, (
        "the dictionary source query never ran, so the startup path this test is meant to "
        "exercise was not exercised at all"
    )

    # A ThreadSanitizer report here means the loader thread published the freshly constructed
    # per-user Counters into the shared chain without ordering, while the profiler signal handler
    # on the startup thread was walking it.
    for filename in ("stderr.log", "clickhouse-server.err.log"):
        report = profiler_race.grep_in_log(
            "ThreadSanitizer: data race", from_host=True, filename=filename, after=40
        )
        assert not report, f"ThreadSanitizer report in {filename}:\n{report}"
