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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # Validate on startup we increase the log level.
        instance.wait_for_log_line("Starting root logger in level trace")
        yield cluster
    finally:
        # Validate on shutdown we set the log level to trace.
        cluster.shutdown()

def test_server_no_logging(started_cluster):
    # Check that the server starts without logging.
    # The log level is set to 'none' in the logger.xml configuration.
    assert instance.http_query("SELECT 1") == "1\n"

    with pytest.raises(Exception):
        # Attempting to wait for HTTP logs should raise an exception since logging is disabled.
        instance.wait_for_log_line("HTTP Request for HTTPHandler-factory", timeout=5)
