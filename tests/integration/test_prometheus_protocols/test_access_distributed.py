"""Privileges on the Prometheus surfaces of a Distributed target.

Every read surface checks the SELECT grant on the target before it looks at the table at all, and
remote write checks the INSERT grant the same way. So a caller without the right learns nothing
else: not the engine behind the name, and not whether the name exists.

The privileged caller runs each request first, so what the restricted caller is denied is always
something that would otherwise have happened.
"""

import json

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

from .prometheus_test_utils import (
    convert_read_request_to_protobuf,
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    get_response_to_remote_read,
    get_response_to_remote_write,
    send_protobuf_to_remote_write,
)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus_distributed.xml",
        "configs/config.d/two_shards_dist.xml",
    ],
    user_configs=[
        "configs/allow_experimental_time_series_table.xml",
        "configs/restricted_users.xml",
    ],
)

DIST = "/dist/api/v1"
# A target whose engine is neither TimeSeries nor Distributed.
BAD_ENGINE = "/bad_engine/api/v1"
# A handler that sets neither database nor table: both come from the query parameters.
DYNAMIC = "/dynamic/api/v1"

NO_SELECT_USER = "prom_no_select"
NO_INSERT_USER = "prom_no_insert"

EVALUATION_TIME = 140

HIDDEN_TABLE = "ts_dist"
MISSING_TABLE = "no_such_table"

INSERT_TEST_DATA = """
INSERT INTO ts_dist (metric_name, tags, time_series) VALUES
    ('m', map('job', 'a', 'host', 'h1'), [(toDateTime64(140, 3), 5)]),
    ('m', map('job', 'a', 'host', 'h3'), [(toDateTime64(140, 3), 50)]),
    ('m', map('job', 'b', 'host', 'h2'), [(toDateTime64(140, 3), 500)]),
    ('m', map('job', 'b', 'host', 'h4'), [(toDateTime64(140, 3), 5000)])
"""


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE shard_0")
        node.query("CREATE DATABASE shard_1")
        node.query("CREATE TABLE shard_0.ts_local ENGINE=TimeSeries")
        node.query("CREATE TABLE shard_1.ts_local ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE ts_dist AS shard_0.ts_local "
            "ENGINE = Distributed(two_shards_dist, '', ts_local, cityHash64(tags['host']))"
        )
        # The same outer schema, so only the engine tells this table apart from `ts_dist`.
        node.query(
            "CREATE TABLE mt_not_ts AS shard_0.ts_local ENGINE = MergeTree ORDER BY tuple()"
        )
        node.query(INSERT_TEST_DATA, settings={"distributed_foreground_insert": 1})
        yield cluster
    finally:
        cluster.shutdown()


def error_code(name):
    """The numeric code of a ClickHouse error, looked up on the server so that no test below
    has to name a magic number of its own."""
    return node.query(
        f"SELECT DISTINCT code FROM system.errors WHERE name = '{name}'",
        settings={"system_events_show_zero_values": 1},
    ).strip()


def as_user(user):
    return {} if user is None else {"user": user, "password": ""}


def credentials_in_url(user):
    return f"?user={user}&password="


def query(handler, user=None, table=None, expect_error=False):
    params = as_user(user)
    if table is not None:
        params.update({"database": "default", "table": table})
    return execute_query_via_http_api(
        node.ip_address,
        9093,
        f"{handler}/query",
        "m",
        EVALUATION_TIME,
        params=params,
        expect_error=expect_error,
    )


def series_count(metric_name):
    return (
        f"SELECT (SELECT count() FROM timeSeriesTags(shard_0.ts_local) WHERE metric_name = '{metric_name}')"
        f" + (SELECT count() FROM timeSeriesTags(shard_1.ts_local) WHERE metric_name = '{metric_name}')"
    )


def test_query_needs_the_select_grant():
    # The privileged caller gets the answer that the restricted one is denied.
    assert len(json.loads(query(DIST))["result"]) == 4

    error = query(DIST, user=NO_SELECT_USER, expect_error=True)
    assert "Not enough privileges" in error, error
    assert "SELECT" in error, error


def test_access_denied_precedes_the_engine_error():
    # A caller that may read the table learns its engine is wrong for this endpoint...
    engine_error = query(BAD_ENGINE, expect_error=True)
    assert "is not TimeSeries" in engine_error, engine_error

    # ...while a caller without the grant learns only that it has no grant. Naming the engine
    # here would tell a probe what hides behind a name it may not read.
    denied = query(BAD_ENGINE, user=NO_SELECT_USER, expect_error=True)
    assert "Not enough privileges" in denied, denied
    assert "TimeSeries" not in denied, denied
    assert "Distributed" not in denied, denied


def test_remote_read_needs_the_select_grant():
    read_request = convert_read_request_to_protobuf("^m$", 0, EVALUATION_TIME)

    # The privileged caller gets as far as the endpoint's own refusal of a Distributed target.
    allowed = get_response_to_remote_read(
        node.ip_address, 9093, f"{DIST}/read", read_request
    )
    assert allowed.headers["X-ClickHouse-Exception-Code"] == error_code("NOT_IMPLEMENTED")

    # The restricted caller is stopped before that, so the refusal never reaches it.
    denied = get_response_to_remote_read(
        node.ip_address,
        9093,
        f"{DIST}/read{credentials_in_url(NO_SELECT_USER)}",
        read_request,
    )
    assert denied.headers["X-ClickHouse-Exception-Code"] == error_code("ACCESS_DENIED")
    assert denied.status_code == requests.codes.forbidden, denied.text
    assert "NOT_IMPLEMENTED" not in denied.text
    assert "Distributed" not in denied.text


def test_remote_write_needs_the_insert_grant():
    denied = get_response_to_remote_write(
        node.ip_address,
        9093,
        f"{DIST}/write{credentials_in_url(NO_INSERT_USER)}",
        convert_time_series_to_protobuf(
            [({"__name__": "denied_metric", "host": "h0"}, {EVALUATION_TIME: 1.0})]
        ),
    )
    assert denied.headers["X-ClickHouse-Exception-Code"] == error_code("ACCESS_DENIED")
    assert denied.status_code == requests.codes.forbidden, denied.text

    # A write the same handler does accept, so the count below is read only after the sink has
    # had its chance to deliver.
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        f"{DIST}/write",
        convert_time_series_to_protobuf(
            [({"__name__": "allowed_metric", "host": "h0"}, {EVALUATION_TIME: 1.0})]
        ),
    )
    assert_eq_with_retry(node, series_count("allowed_metric"), "1")
    assert node.query(series_count("denied_metric")).strip() == "0"


def test_dynamic_table_hides_whether_the_table_exists():
    # For a caller that may read them the two names are plainly different.
    assert len(json.loads(query(DYNAMIC, table=HIDDEN_TABLE))["result"]) == 4
    missing = query(DYNAMIC, table=MISSING_TABLE, expect_error=True)
    assert "does not exist" in missing, missing

    # Without the SELECT grant they are the same error, differing only in the name asked about.
    hidden_denied = query(
        DYNAMIC, user=NO_SELECT_USER, table=HIDDEN_TABLE, expect_error=True
    )
    missing_denied = query(
        DYNAMIC, user=NO_SELECT_USER, table=MISSING_TABLE, expect_error=True
    )
    assert "Not enough privileges" in hidden_denied, hidden_denied
    assert hidden_denied.replace(HIDDEN_TABLE, "<name>") == missing_denied.replace(
        MISSING_TABLE, "<name>"
    )
    assert "does not exist" not in hidden_denied, hidden_denied
    assert "Distributed" not in hidden_denied, hidden_denied
