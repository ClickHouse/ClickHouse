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
    node.query(
        "CREATE TABLE prometheus (samples Array(Tuple(DateTime64(6), Float64))) "
        "ENGINE = TimeSeries SETTINGS version = 11"
    )

    # Insert 40 samples relative to now().
    node.query(
        """
        INSERT INTO prometheus (metric_name, tags, samples)
        SELECT 'cpu_usage', map('host', 'h1'), groupArray((toDateTime64(now(), 6) - interval (number * 60) second, number * 1.0))
        FROM numbers(40)
        """
    )

    # This UTC instant falls in January in Los Angeles but February in UTC. The generated v11
    # monthly partition key must be UTC and keep the DateTime64 scale when normalizing bucket.
    boundary_sample = "toDateTime64('2025-02-01 00:30:00.123456', 6, 'UTC')"
    node.query(
        f"INSERT INTO prometheus (metric_name, tags, samples) VALUES "
        f"('cpu_usage', map('host', 'h1'), [({boundary_sample}, 42.)])"
    )

    utc_partition_count = node.query(
        "SELECT count() FROM system.parts WHERE database = currentDatabase() "
        "AND table LIKE '.inner_id.samples.%' AND active AND partition = '202502'"
    ).strip()
    assert int(utc_partition_count) > 0, f"Expected the boundary sample in UTC partition 202502, got {utc_partition_count}"

    fractional_sample_count = node.query(
        "SELECT count() FROM timeSeriesSamples(prometheus) ARRAY JOIN samples AS sample "
        f"WHERE sample.1 = {boundary_sample}"
    ).strip()
    assert fractional_sample_count == "1", f"Expected one DateTime64(6) boundary sample, got {fractional_sample_count}"

    # Initial query under America/Los_Angeles.
    count_before = node.query(
        "SELECT sum(length(time_series)) "
        "FROM timeSeriesSelector(currentDatabase(), 'prometheus', 'cpu_usage', now() - 3600, now())"
    ).strip()
    assert count_before == "40", f"Expected 40 samples before restart, got {count_before}"

    # Restart the server with another timezone (Asia/Tokyo).
    node.stop_clickhouse()
    node.replace_in_config(CONFIG_PATH, "America/Los_Angeles", "Asia/Tokyo")
    node.start_clickhouse()

    # Range query after server timezone change must still read all 40 samples.
    count_after = node.query(
        "SELECT sum(length(time_series)) "
        "FROM timeSeriesSelector(currentDatabase(), 'prometheus', 'cpu_usage', now() - 3600, now())"
    ).strip()
    assert count_after == "40", (
        f"a range query returned {count_after} of the 40 samples in that range after the server timezone changed"
    )

    partition_after = node.query(
        "SELECT count() FROM system.parts WHERE database = currentDatabase() "
        "AND table LIKE '.inner_id.samples.%' AND active AND partition = '202502'"
    ).strip()
    assert int(partition_after) > 0, f"Expected the UTC partition to remain 202502 after restart, got {partition_after}"

    fractional_sample_after = node.query(
        "SELECT count() FROM timeSeriesSamples(prometheus) ARRAY JOIN samples AS sample "
        f"WHERE sample.1 = {boundary_sample}"
    ).strip()
    assert fractional_sample_after == "1", f"Expected DateTime64(6) sample after restart, got {fractional_sample_after}"

    # Clean up.
    node.query("DROP TABLE prometheus SYNC")
    node.replace_in_config(CONFIG_PATH, "Asia/Tokyo", "America/Los_Angeles")
