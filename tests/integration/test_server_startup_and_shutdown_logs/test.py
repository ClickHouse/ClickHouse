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
        assert instance.contains_in_log("Starting root logger in level trace")
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

    instance.start_clickhouse()
