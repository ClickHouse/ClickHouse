"""
FORMAT Prometheus vs OpenMetrics on TimeSeries rows ingested via remote write only.

Isolated from test_compliance.py so extra samples do not share the compliance table
with test_promql_compliance (same job/instance label namespace).
"""

import pytest

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import (
    assert_prometheus_openmetrics_exposition_equivalent,
    convert_time_series_to_protobuf,
    send_protobuf_to_remote_write,
)
from .generate_compliance_data import DATA_END


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_read=(9093, "/read"),
    handle_prometheus_remote_write=(9093, "/write"),
    with_prometheus_receiver=True,
)

_TEXT_EXPORT_METRIC = "demo_http_requests_total"

_TEXT_EXPORT_TIMESERIES_QUERY = f"""
SELECT * FROM (
    SELECT
        CAST(tags.metric_name AS String) AS name,
        data.value AS value,
        coalesce(CAST(metrics.help AS String), '') AS help,
        coalesce(CAST(metrics.type AS String), '') AS type,
        mapFilter((k, v) -> (k != '__name__'), tags.tags) AS labels,
        toUnixTimestamp64Milli(data.timestamp) AS timestamp,
        coalesce(CAST(metrics.unit AS String), '') AS unit
    FROM timeSeriesData(prometheus) AS data
    INNER JOIN timeSeriesTags(prometheus) AS tags ON data.id = tags.id
    LEFT JOIN timeSeriesMetrics(prometheus) AS metrics ON tags.metric_name = metrics.metric_family_name
    WHERE tags.metric_name = '{_TEXT_EXPORT_METRIC}'
    ORDER BY data.timestamp, tags.id
    LIMIT 12
) ORDER BY timestamp
"""


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        yield cluster
    finally:
        cluster.shutdown()


def test_compliance_ingest_prometheus_openmetrics_text_export():
    """
    FORMAT Prometheus vs OpenMetrics on rows inserted only via Prometheus remote write
    (snappy-compressed PRW to ClickHouse HTTP /write).
    """
    samples = {
        float(DATA_END - 30): 1027.0,
        float(DATA_END - 15): 1038.0,
    }
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        convert_time_series_to_protobuf(
            [
                (
                    {
                        "__name__": _TEXT_EXPORT_METRIC,
                        "instance": "demo.promlabs.com:10000",
                        "job": "demo",
                        "method": "POST",
                        "status": "200",
                    },
                    samples,
                ),
            ]
        ),
    )

    prometheus_text = node.query(f"{_TEXT_EXPORT_TIMESERIES_QUERY}\nFORMAT Prometheus")
    openmetrics_text = node.query(f"{_TEXT_EXPORT_TIMESERIES_QUERY}\nFORMAT OpenMetrics")
    assert_prometheus_openmetrics_exposition_equivalent(prometheus_text, openmetrics_text)
