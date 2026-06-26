"""
FORMAT Prometheus vs OpenMetrics on TimeSeries rows inserted directly into the table engine.

Isolated from test_compliance.py so extra samples do not share the compliance table
with test_promql_compliance (same job/instance label namespace).
"""

import pytest

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import (
    assert_prometheus_openmetrics_exposition_equivalent,
)
from .generate_compliance_data import DATA_END


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

_TEXT_EXPORT_METRIC = "demo_http_requests_total"

# Reusable parameterized view that reshapes the TimeSeries target tables into the
# column layout FORMAT Prometheus / OpenMetrics resolve by name. The TimeSeries engine
# does not support reading via SELECT directly, so the view joins the inner
# timeSeriesSamples / timeSeriesTags / timeSeriesMetrics table functions. See
# docs/en/interfaces/formats/OpenMetrics.md "Exporting from a TimeSeries table".
_EXPORT_VIEW = """
CREATE VIEW prometheus_exposition AS
SELECT
    CAST(tags.metric_name AS String)                  AS name,
    samples.value                                     AS value,
    coalesce(CAST(metrics.help AS String), '')        AS help,
    coalesce(CAST(metrics.type AS String), '')        AS type,
    coalesce(CAST(metrics.unit AS String), '')        AS unit,
    CAST(mapFilter((k, v) -> (k != '__name__'), tags.tags) AS Map(String, String)) AS labels,
    toUnixTimestamp64Milli(samples.timestamp)         AS timestamp
FROM timeSeriesSamples(prometheus) AS samples
INNER JOIN timeSeriesTags(prometheus) AS tags ON samples.id = tags.id
LEFT JOIN timeSeriesMetrics(prometheus) AS metrics ON tags.metric_name = metrics.metric_family_name
WHERE tags.metric_name = {metric:String}
"""

_TEXT_EXPORT_QUERY = (
    f"SELECT * FROM prometheus_exposition(metric = '{_TEXT_EXPORT_METRIC}')"
    " ORDER BY name, timestamp"
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        node.query(_EXPORT_VIEW)
        yield cluster
    finally:
        cluster.shutdown()


def test_compliance_ingest_prometheus_openmetrics_text_export():
    """
    FORMAT Prometheus vs OpenMetrics on rows inserted directly into the TimeSeries
    table engine (INSERT INTO ... (metric_name, tags, time_series), no remote write),
    exported through the reusable prometheus_exposition view.
    """
    labels = {
        "instance": "demo.promlabs.com:10000",
        "job": "demo",
        "method": "POST",
        "status": "200",
    }
    samples = {
        DATA_END - 30: 1027.0,
        DATA_END - 15: 1038.0,
    }
    tags_sql = "{" + ", ".join(f"'{k}': '{v}'" for k, v in labels.items()) + "}"
    time_series_sql = ", ".join(
        f"(toDateTime64({int(ts)}, 3), {value})" for ts, value in samples.items()
    )
    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        f" ('{_TEXT_EXPORT_METRIC}', {tags_sql}, [{time_series_sql}])"
    )

    prometheus_text = node.query(f"{_TEXT_EXPORT_QUERY}\nFORMAT Prometheus")
    openmetrics_text = node.query(f"{_TEXT_EXPORT_QUERY}\nFORMAT OpenMetrics")
    assert_prometheus_openmetrics_exposition_equivalent(prometheus_text, openmetrics_text)
