import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/timezone.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    stay_alive=True,
)

CONFIG_PATH = "/etc/clickhouse-server/config.d/timezone.xml"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_timeseries_partition_key_timezone(start_cluster):
    node.query("CREATE TABLE prometheus ENGINE = TimeSeries")

    # Insert 40 samples relative to now().
    node.query(
        """
        INSERT INTO prometheus (metric_name, tags, samples)
        SELECT 'cpu_usage', map('host', 'h1'), groupArray((now() - interval (number * 60) second, number * 1.0))
        FROM numbers(40)
        """
    )

    # Initial query under America/Los_Angeles.
    count_before = node.query(
        "SELECT count() FROM timeSeriesSelector(currentDatabase(), 'prometheus', 'cpu_usage', now() - 3600, now())"
    ).strip()
    assert count_before == "40", f"Expected 40 samples before restart, got {count_before}"

    # Restart the server with another timezone (Asia/Tokyo).
    node.stop_clickhouse()
    node.replace_in_config(CONFIG_PATH, "America/Los_Angeles", "Asia/Tokyo")
    node.start_clickhouse()

    # Range query after server timezone change must still read all 40 samples.
    count_after = node.query(
        "SELECT count() FROM timeSeriesSelector(currentDatabase(), 'prometheus', 'cpu_usage', now() - 3600, now())"
    ).strip()
    assert count_after == "40", (
        f"a range query returned {count_after} of the 40 samples in that range after the server timezone changed"
    )

    # Clean up.
    node.query("DROP TABLE prometheus SYNC")
    node.replace_in_config(CONFIG_PATH, "Asia/Tokyo", "America/Los_Angeles")
