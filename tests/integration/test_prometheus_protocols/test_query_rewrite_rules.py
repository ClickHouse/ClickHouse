"""Query rewrite rules must never be applied to the SQL synthesized by the Prometheus protocols."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster

from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    send_protobuf_to_remote_write,
)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)

# A rule name that does not exist on the server. `query_rules` is applied in `executeQuery`, and a
# listed rule that does not exist fails the query with `REWRITE_RULE_DOESNT_EXIST`. So requesting
# this rule is a template-independent probe: it fails every query rewrite rules are applied to, and
# is a no-op for every query they are not applied to.
MISSING_RULE = "no_such_rewrite_rule"


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_protobuf_to_remote_write(
            node.ip_address,
            9093,
            "/write",
            convert_time_series_to_protobuf(
                [({"__name__": "cpu_usage", "host": "server1"}, {1000: 0.5})]
            ),
        )
        yield
    finally:
        cluster.shutdown()


def get_json_from_api(path):
    response = requests.get(f"http://{node.ip_address}:9093{path}")
    assert (
        response.status_code == 200
    ), f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/query?query=cpu_usage&time=1000",
        "/api/v1/query_range?query=cpu_usage&start=1000&end=1030&step=15",
        "/api/v1/series?match[]=cpu_usage",
        "/api/v1/labels",
        "/api/v1/label/host/values",
        "/api/v1/metadata",
    ],
)
def test_http_api_ignores_query_rules(path):
    # The client submits no SQL at all - only a PromQL expression or `match[]` selectors - so the
    # SQL that `PrometheusHTTPProtocolAPI` synthesizes must run with `query_rules` cleared.
    separator = "&" if "?" in path else "?"
    get_json_from_api(f"{path}{separator}query_rules={MISSING_RULE}")


def test_remote_write_ignores_query_rules():
    # The same for the `INSERT ... FORMAT Native` synthesized from the protobuf payload of a
    # remote-write request.
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        f"/write?query_rules={MISSING_RULE}",
        convert_time_series_to_protobuf(
            [({"__name__": "cpu_usage", "host": "server2"}, {2000: 0.7})]
        ),
    )
    assert (
        node.query(
            "SELECT count() FROM timeSeriesTags(prometheus) WHERE tags['host'] = 'server2'"
        ).strip()
        == "1"
    )


def test_submitted_sql_still_matches_rules():
    # The clearing above is scoped to the synthesized SQL: SQL the client itself submits over the
    # HTTP interface keeps `query_rules` and is matched as usual, including a missing rule name.
    response = requests.get(
        f"http://{node.ip_address}:8123/",
        params={"query": "SELECT 1", "query_rules": MISSING_RULE},
    )
    assert response.status_code != 200
    assert "REWRITE_RULE_DOESNT_EXIST" in response.text
