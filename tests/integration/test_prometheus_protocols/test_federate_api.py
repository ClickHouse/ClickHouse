"""Tests for the Prometheus /federate endpoint."""

import re
import struct
import time

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
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

STALE_NAN = struct.unpack("<d", struct.pack("<Q", 0x7FF0000000000002))[0]

now = 0


def ms(seconds_ago):
    return (now - seconds_ago) * 1000


@pytest.fixture(scope="module", autouse=True)
def setup():
    global now
    try:
        cluster.start()
        # Whole seconds, taken after startup so the samples stay inside the 5-minute window.
        now = int(time.time())
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        time_series = [
            ({"__name__": "fed_cpu", "host": "a", "instance": "i1"}, {now - 120: 1.0, now - 60: 2.5}),
            ({"__name__": "fed_cpu", "host": "b"}, {now - 30: 1234567.0}),
            ({"__name__": "fed_mem", "host": "a"}, {now - 10: 0.00001}),
            ({"__name__": "fed_old", "host": "a"}, {now - 900: 1.0}),
            ({"__name__": "fed_stale", "host": "a"}, {now - 90: 5.0, now - 30: STALE_NAN}),
            ({"__name__": "fed.dotted", "label.name": 'a"b\\c\nd'}, {now - 20: -3.0}),
        ]
        send_protobuf_to_remote_write(
            node.ip_address, 9093, "/write", convert_time_series_to_protobuf(time_series)
        )
        assert_eq_with_retry(node, "SELECT count() FROM timeSeriesData(prometheus)", "8")
        yield cluster
    finally:
        cluster.shutdown()


def federate(params, expected_status=200):
    response = requests.get(f"http://{node.ip_address}:9093/federate", params=params)
    assert response.status_code == expected_status, response.text
    return response


def test_federate_text_format():
    response = federate({"match[]": ['{__name__=~"fed_.*"}', "fed_cpu", '{__name__="fed.dotted"}']})
    assert response.headers["Content-Type"] == "text/plain; version=0.0.4; charset=utf-8; escaping=underscores"
    # Sorted by the original metric name, so "fed.dotted" comes first.
    assert response.text == (
        "# TYPE fed_dotted untyped\n"
        f'fed_dotted{{label_name="a\\"b\\\\c\\nd",instance=""}} -3 {ms(20)}\n'
        "# TYPE fed_cpu untyped\n"
        f'fed_cpu{{host="a",instance="i1"}} 2.5 {ms(60)}\n'
        f'fed_cpu{{host="b",instance=""}} 1.234567e+06 {ms(30)}\n'
        "# TYPE fed_mem untyped\n"
        f'fed_mem{{host="a",instance=""}} 1e-05 {ms(10)}\n'
    )


def test_federate_matches_instant_query():
    text = federate({"match[]": "fed_cpu"}).text
    federated = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        match = re.fullmatch(r'fed_cpu\{host="(\w+)",instance="\w*"\} (\S+) \d+', line)
        federated[match.group(1)] = float(match.group(2))

    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/query", params={"query": "fed_cpu"}
    )
    queried = {
        series["metric"]["host"]: float(series["value"][1])
        for series in response.json()["data"]["result"]
    }
    assert federated == queried == {"a": 2.5, "b": 1234567.0}


def test_federate_without_match_is_empty():
    assert federate({}).text == ""


def test_federate_bad_selector():
    response = federate({"match[]": "rate(fed_cpu[5m])"}, expected_status=400)
    assert response.headers["Content-Type"] == "application/json"
    federate({"match[]": "fed_cpu{"}, expected_status=400)
