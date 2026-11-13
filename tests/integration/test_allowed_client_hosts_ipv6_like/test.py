import urllib.error
import urllib.parse
import urllib.request

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config.xml"])


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def query_as_user(user, forwarded_address):
    params = urllib.parse.urlencode({"query": "SELECT 1", "user": user})
    request = urllib.request.Request(
        f"http://{node.ip_address}:8123/?{params}",
        headers={"X-Forwarded-For": forwarded_address},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.read().decode("utf-8")


def test_host_like_ipv6_short_first_hextets(started_cluster):
    user = "user_allowed_client_hosts_ipv6"
    cases = [
        ("1", "2345"),
        ("12", "3456"),
        ("123", "4567"),
        ("1234", "5678"),
    ]
    patterns = ", ".join(
        f"'{first}:{second}:0:0:0:0:0:%'" for first, second in cases
    )

    node.query(f"DROP USER IF EXISTS {user}")
    node.query(
        f"CREATE USER {user} IDENTIFIED WITH no_password HOST LIKE {patterns}"
    )
    try:
        for first, second in cases:
            address = f"{first}:{second}:0:0:0:0:0:1"
            assert query_as_user(user, address) == "1\n"

        with pytest.raises(urllib.error.HTTPError) as error:
            query_as_user(user, "1:2345:0:0:0:0:1:1")
        assert error.value.code == 403
    finally:
        node.query(f"DROP USER IF EXISTS {user}")
