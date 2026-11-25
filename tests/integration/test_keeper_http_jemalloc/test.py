#!/usr/bin/env python3

import pytest
import requests

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

JEMALLOC_HTTP_PORT = 9182


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start(connection_timeout=450.0)
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def leader(started_cluster):
    return keeper_utils.get_leader(cluster, [node1, node2, node3])


@pytest.fixture(scope="module")
def has_jemalloc(leader):
    url = f"http://{leader.ip_address}:{JEMALLOC_HTTP_PORT}/jemalloc/status"
    return requests.get(url).status_code != 501


def get_url(node, path):
    return f"http://{node.ip_address}:{JEMALLOC_HTTP_PORT}{path}"


def assert_501(response):
    assert response.status_code == 501
    assert "not available" in response.text.lower()


def test_jemalloc_web_ui(leader):
    response = requests.get(get_url(leader, "/jemalloc"))
    assert response.status_code == 200
    assert "text/html" in response.headers["Content-Type"]
    assert "<!DOCTYPE html>" in response.text
    assert "JEMALLOC_CONFIG" in response.text
    assert "keeper" in response.text


def test_jemalloc_trailing_slash_redirects(leader):
    response = requests.get(get_url(leader, "/jemalloc/"), allow_redirects=False)
    assert response.status_code == 301
    assert response.headers["Location"] == "/jemalloc"


def test_jemalloc_stats(leader, has_jemalloc):
    response = requests.get(get_url(leader, "/jemalloc/stats"))
    if has_jemalloc:
        assert response.status_code == 200
        assert "text/plain" in response.headers["Content-Type"]
        text = response.text.lower()
        assert "jemalloc" in text or "allocated" in text
    else:
        assert_501(response)


def test_jemalloc_status(leader, has_jemalloc):
    response = requests.get(get_url(leader, "/jemalloc/status"))
    if has_jemalloc:
        assert response.status_code == 200
        assert "application/json" in response.headers["Content-Type"]
        status = response.json()
        for key in ("prof_enabled", "prof_active", "thread_active_init", "lg_sample"):
            assert key in status
    else:
        assert_501(response)


def test_jemalloc_profile_bad_format(leader, has_jemalloc):
    response = requests.get(get_url(leader, "/jemalloc/profile?format=bad"))
    if has_jemalloc:
        assert response.status_code == 400
        assert "Unknown format" in response.text
    else:
        assert_501(response)


def test_jemalloc_unknown_api_path(leader):
    response = requests.get(get_url(leader, "/jemalloc/nonexistent"))
    assert response.status_code == 404


@pytest.mark.parametrize("path", ["/jemalloc/stats", "/jemalloc/status"])
def test_jemalloc_head(leader, has_jemalloc, path):
    response = requests.head(get_url(leader, path))
    if has_jemalloc:
        assert response.status_code == 200
    else:
        assert response.status_code == 501
    assert len(response.content) == 0


def test_jemalloc_head_profile(leader, has_jemalloc):
    response = requests.head(get_url(leader, "/jemalloc/profile"))
    if not has_jemalloc:
        assert response.status_code == 501
    else:
        status = requests.get(get_url(leader, "/jemalloc/status")).json()
        if status.get("prof_enabled", False):
            assert response.status_code == 200
        else:
            assert response.status_code == 500
    assert len(response.content) == 0


@pytest.mark.parametrize("fmt", ["collapsed", "raw"])
def test_jemalloc_profile_success(leader, has_jemalloc, fmt):
    if not has_jemalloc:
        pytest.skip("jemalloc is not available in this build")

    status = requests.get(get_url(leader, "/jemalloc/status")).json()
    if not status.get("prof_enabled", False):
        pytest.skip("jemalloc profiling is not enabled (opt.prof=false)")

    url = get_url(leader, f"/jemalloc/profile?format={fmt}") if fmt != "collapsed" else get_url(leader, "/jemalloc/profile")
    response = requests.get(url)
    assert response.status_code == 200
    assert "text/plain" in response.headers["Content-Type"]
    if fmt == "raw":
        assert len(response.text) > 0
