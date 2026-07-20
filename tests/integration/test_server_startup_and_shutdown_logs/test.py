import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/logger.xml",
    ],
    user_configs=[],
    stay_alive=True,
)

# Separate instances validate that the console log level is restored after startup.
instance_console_unset = cluster.add_instance(
    "node_console_unset",
    main_configs=[
        "configs/config.d/logger_console_unset.xml",
    ],
    stay_alive=True,
)
instance_console_set = cluster.add_instance(
    "node_console_set",
    main_configs=[
        "configs/config.d/logger_console_set.xml",
    ],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # Validate on startup we increase the log level.
        assert instance.contains_in_log("Starting root logger in level trace")
        assert instance.contains_in_log("Starting console logger in level trace")

        # The console log level is raised at startup and then restored afterwards.
        # When console_log_level is unset it reverts to follow logger.level ...
        assert instance_console_unset.contains_in_log(
            "Starting console logger in level trace"
        )
        assert instance_console_unset.contains_in_log(
            "Restored console logger level to logger.level"
        )
        # ... and when it is explicitly configured it is restored to that value.
        assert instance_console_set.contains_in_log(
            "Starting console logger in level trace"
        )
        assert instance_console_set.contains_in_log(
            "Restored console logger level to warning"
        )
        yield cluster
    finally:
        cluster.shutdown()

def test_server_no_logging(started_cluster):
    # Check that the server starts without logging.
    # The log level is set to 'none' in the logger.xml configuration.
    assert instance.http_query("SELECT 1") == "1\n"

    assert not instance.contains_in_log("HTTP Request for HTTPHandler-factory")

    # shutdown Clickhouse
    instance.stop_clickhouse()

    assert instance.contains_in_log("Set root logger in level trace before shutdown")
    assert instance.contains_in_log("Set console logger in level trace before shutdown")

    instance.start_clickhouse()
