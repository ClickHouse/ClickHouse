"""A cluster entry with no <default_database> is local on this node, so the sink writes it and
the read rewrite reads it in-process on the caller's context - which resolves an undeclared
shard-local database to the caller's current one, not to the probe connection's. The probe has
to ask each replica about the table that replica will actually be given."""

import json

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    get_response_to_remote_write,
    send_protobuf_to_remote_write,
)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus_local_shard.xml",
        "configs/config.d/local_shard_dist.xml",
    ],
    user_configs=[
        "configs/allow_experimental_time_series_table.xml",
        "configs/user_default_database.xml",
    ],
)

START_TIME = 1724112000

# The probe's own connection authenticates as `default`, whose current database is `default`;
# this caller's is `metrics`, and that is where its writes and reads land.
CALLER = "?user=prom_metrics&password="
CALLER_PARAMS = {"user": "prom_metrics", "password": ""}


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE metrics")

        node.query("CREATE TABLE metrics.ts_local ENGINE=TimeSeries")
        # Same outer schema, wrong engine, under the name the probe connection resolves.
        node.query(
            "CREATE TABLE default.ts_local AS metrics.ts_local ENGINE = MergeTree ORDER BY tuple()"
        )
        node.query(
            "CREATE TABLE metrics.prom_local AS metrics.ts_local "
            "ENGINE = Distributed(local_shard_dist, '', ts_local)"
        )

        # The same pair the other way round: healthy where the probe used to look, wrong engine
        # where the sink actually writes.
        node.query("CREATE TABLE default.ts_swap ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE metrics.ts_swap AS default.ts_swap ENGINE = MergeTree ORDER BY tuple()"
        )
        node.query(
            "CREATE TABLE default.prom_swap AS default.ts_swap "
            "ENGINE = Distributed(local_shard_dist, '', ts_swap)"
        )
        yield cluster
    finally:
        cluster.shutdown()


def one_sample(metric_name):
    return convert_time_series_to_protobuf(
        [({"__name__": metric_name, "host": "h0"}, {START_TIME: 1.0})]
    )


def test_remote_write_checks_the_table_the_sink_writes():
    """The engine of `default.ts_local` says nothing about this write: the local shard's rows go
    to `metrics.ts_local`, so that is the table the probe has to verify."""
    send_protobuf_to_remote_write(
        node.ip_address, 9093, f"/local/write{CALLER}", one_sample("local_metric")
    )
    assert_eq_with_retry(
        node,
        "SELECT count() FROM timeSeriesTags(metrics.ts_local) WHERE metric_name = 'local_metric'",
        "1",
    )
    assert int(node.query("SELECT count() FROM default.ts_local")) == 0


def test_query_reads_the_table_the_rewrite_reads():
    """The read runs the local shard in-process too, so it resolves the same way the write did."""
    result = json.loads(
        execute_query_via_http_api(
            node.ip_address,
            9093,
            "/local_api/query",
            "local_metric",
            START_TIME,
            params=CALLER_PARAMS,
        )
    )["result"]
    assert [sample["value"][1] for sample in result] == ["1"]


def test_remote_write_is_refused_when_only_the_probes_database_is_healthy():
    """`default.ts_swap` is a TimeSeries table of the wrapper's type, and none of the samples
    would have reached it: the sink writes `metrics.ts_swap`, whose engine cannot hold them.
    """
    response = get_response_to_remote_write(
        node.ip_address, 9093, f"/swap/write{CALLER}", one_sample("swap_metric")
    )
    assert response.status_code >= 400
    assert "UNEXPECTED_TABLE_ENGINE" in response.text
    assert int(node.query("SELECT count() FROM metrics.ts_swap")) == 0
    assert int(node.query("SELECT count() FROM timeSeriesTags(default.ts_swap)")) == 0
