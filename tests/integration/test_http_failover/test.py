from contextlib import nullcontext as does_not_raise

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, exec_query_with_retry

ACCESSIBLE_IPV4 = "10.5.172.10"
OTHER_ACCESSIBLE_IPV4 = "10.5.172.20"
NOT_ACCESSIBLE_IPV4 = "10.5.172.11"

ACCESSIBLE_IPV6 = "2001:3984:3989::1:1000"
NOT_ACCESSIBLE_IPV6 = "2001:3984:3989::1:1001"

DST_NODE_IPV4 = ACCESSIBLE_IPV4
DST_NODE_IPV6 = ACCESSIBLE_IPV6
SRC_NODE_IPV6 = "2001:3984:3989::1:2000"


cluster = ClickHouseCluster(__file__)

# Destination node
dst_node = cluster.add_instance(
    "dst_node",
    with_zookeeper=True,
    ipv4_address=DST_NODE_IPV4,
    ipv6_address=DST_NODE_IPV6,
    main_configs=["configs/listen.xml"],
)
# Source node
src_node = cluster.add_instance(
    "src_node",
    with_zookeeper=True,
    ipv6_address=SRC_NODE_IPV6,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()
        pass


@pytest.fixture
def dst_node_addrs(started_cluster, request):
    src_node.set_hosts([(ip, "dst_node") for ip in request.param])
    src_node.query("SYSTEM DROP DNS CACHE")

    yield

    # Clear static DNS entries and all keep alive connections
    src_node.set_hosts([])
    src_node.query("SYSTEM DROP DNS CACHE")
    src_node.query("SYSTEM DROP CONNECTIONS CACHE")


@pytest.mark.parametrize(
    "dst_node_addrs, expectation",
    [
        ((ACCESSIBLE_IPV4, ACCESSIBLE_IPV6), does_not_raise()),
        ((NOT_ACCESSIBLE_IPV4, ACCESSIBLE_IPV6), does_not_raise()),
        ((ACCESSIBLE_IPV4, NOT_ACCESSIBLE_IPV6), does_not_raise()),
        (
            (NOT_ACCESSIBLE_IPV4, NOT_ACCESSIBLE_IPV6),
            pytest.raises(QueryRuntimeException),
        ),
    ],
    indirect=["dst_node_addrs"],
)
def test_url_destination_host_with_multiple_addrs(dst_node_addrs, expectation):
    with expectation:
        result = src_node.query(
            "SELECT * FROM url('http://dst_node:8123/?query=SELECT+42', TSV, 'column1 UInt32')",
            settings={"http_max_tries": "3"},
        )
        assert result == "42\n"


def test_url_invalid_hostname(started_cluster):
    with pytest.raises(QueryRuntimeException):
        src_node.query(
            "SELECT count(*) FROM url('http://notvalidhost:8123/?query=SELECT+1', TSV, 'column1 UInt32');"
        )


def test_url_ip_change(started_cluster):
    assert (
        src_node.query(
            "SELECT * FROM url('http://dst_node:8123/?query=SELECT+42', TSV, 'column1 UInt32')"
        )
        == "42\n"
    )

    started_cluster.restart_instance_with_ip_change(dst_node, OTHER_ACCESSIBLE_IPV4)

    # Ensure that only new IPV4 address is accessible
    src_node.set_hosts(
        [(OTHER_ACCESSIBLE_IPV4, "dst_node"), (NOT_ACCESSIBLE_IPV6, "dst_node")]
    )
    src_node.query("SYSTEM DROP DNS CACHE")

    assert (
        src_node.query(
            "SELECT * FROM url('http://dst_node:8123/?query=SELECT+42', TSV, 'column1 UInt32')"
        )
        == "42\n"
    )
