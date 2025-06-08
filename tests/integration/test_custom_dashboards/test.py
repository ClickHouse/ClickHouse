import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

default = cluster.add_instance("default")
custom = cluster.add_instance("custom", main_configs=["configs/config.d/overrides.xml"])


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_custom_dashboards():
    assert int(default.query("select count()>10 from system.dashboards")) == 1
    assert int(custom.query("select count() from system.dashboards")) == 1
    assert (
        default.query(
            "select normalizeQuery(query) from system.dashboards where dashboard = 'Overview' and title = 'Queries/second'"
        ).strip()
        == """
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_Query) FROM merge(?..) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32}
    """.strip()
    )
    custom.query(
        "select normalizeQuery(query) from system.dashboards where dashboard = 'Overview' and title = 'Queries/second'"
    ).strip == """
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_Query) FROM system.metric_log WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32}
    """.strip()
