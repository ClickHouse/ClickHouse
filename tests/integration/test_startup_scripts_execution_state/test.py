import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
good = cluster.add_instance(
    "good",
    main_configs=["config/users.xml", "config/good_script.xml"],
    stay_alive=True,
)
bad = cluster.add_instance(
    "bad",
    main_configs=["config/users.xml", "config/bad_script.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_startup_execution_state(start_cluster):
    """
    Making sure that the StartupScriptsExecutionState metric is set correctly
    and that the dimensional metric startup_scripts_failure_reason is recorded.
    """

    STATE_SUCCESS = 1
    STATE_FAILURE = 2

    def assert_startup_script_failed():
        assert (
            int(
                bad.query(
                    "SELECT value FROM system.metrics WHERE metric = 'StartupScriptsExecutionState'"
                ).strip()
            )
            == STATE_FAILURE
        )

    assert (
        int(
            good.query(
                "SELECT value FROM system.metrics WHERE metric = 'StartupScriptsExecutionState'"
            ).strip()
        )
        == STATE_SUCCESS
    )
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

    assert int(
        bad.query(
            """
            SELECT value
            FROM system.dimensional_metrics
            WHERE 1
                AND metric = 'startup_scripts_failure_reason'
                AND labels['error_name'] = 'UNKNOWN_TABLE'
            """
        ).strip()
    ) == 1
