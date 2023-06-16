import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import exec_query_with_retry
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)

valid_ipv4 = "10.5.172.11"
wrong_ipv6 = "2001:3984:3989::1:1118"

# Destination node
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    ipv4_address=valid_ipv4,
)
# Source node
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1219",
)


# Testing different scenarios, when the http endpoint have several ips.
def _prepare(node):
    node.exec_in_container(
        (["bash", "-c", "echo '{}' node1 >> /etc/hosts".format(valid_ipv4)])
    )
    node.exec_in_container(
        (["bash", "-c", "echo '{}' node1 >> /etc/hosts".format(wrong_ipv6)])
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        _prepare(node2)
        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()
        pass


def test_ip_url_with_multiple_ips(started_cluster):
    node1.query(
        "CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE = MergeTree ORDER BY column2;"
    )

    node2.query(
        "INSERT INTO FUNCTION url('http://node1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);"
    )
    assert node1.query("SELECT count(*) FROM test_table;") == "1\n"
    assert (
        node2.query(
            "SELECT count(*) FROM url('http://node1:8123/?query=SELECT+1', CSV, 'column2 UInt32', headers('Accept'='text/csv; charset=utf-8'));"
        )
        == "1\n"
    )


def test_url_invalid_hostname(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_table")

    node1.query(
        "CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE = MergeTree ORDER BY column2;"
    )
    with pytest.raises(QueryRuntimeException):
        node2.query(
            "INSERT INTO FUNCTION url('http://notvalidhost:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);"
        )

    assert node1.query("SELECT count(*) FROM test_table;") == "0\n"

    with pytest.raises(QueryRuntimeException):
        node2.query(
            "SELECT count(*) FROM url('http://notvalidhost:8123/?query=SELECT+1', CSV, 'column2 UInt32', headers('Accept'='text/csv; charset=utf-8'));"
        )


def test_url_ip_change(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_table")

    node1.query(
        "CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE = MergeTree ORDER BY column2;"
    )

    node2.query(
        "INSERT INTO FUNCTION url('http://node1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('first', 1);"
    )
    assert node1.query("SELECT count(*) FROM test_table;") == "1\n"

    new_ip_host = "10.5.172.14"

    started_cluster.restart_instance_with_ip_change(node1, new_ip_host)

    node2.exec_in_container(
        (["bash", "-c", "echo '{}' node1 >> /etc/hosts".format(new_ip_host)])
    )

    node2.query("SYSTEM DROP DNS CACHE")

    exec_query_with_retry(
        node2,
        "INSERT INTO FUNCTION url('http://node1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('second', 2);",
    )

    node1.query("SELECT count(*) FROM test_table") == "2\n"
    node2.query(
        "SELECT count(*) FROM url('http://node1:8123/?query=SELECT+*+FROM+test_table+FORMAT+CSV', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8'));"
    ) == "2\n"


def test_url_inaccessible_hostname(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_table")
    node1.query(
        "CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE = MergeTree ORDER BY column2;"
    )

    new_ip_host = "10.5.172.15"
    started_cluster.restart_instance_with_ip_change(node1, new_ip_host)

    with pytest.raises(QueryRuntimeException):
        node2.query(
            "INSERT INTO FUNCTION url('http://node1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);"
        )

    assert node1.query("SELECT count(*) FROM test_table;") == "0\n"

    with pytest.raises(QueryRuntimeException):
        node2.query(
            "SELECT count(*) FROM url('http://node1:8123/?query=SELECT+1', CSV, 'column2 UInt32', headers('Accept'='text/csv; charset=utf-8'));"
        )


def test_url_already_resolved(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_table")
    node1.query(
        "CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE = MergeTree ORDER BY column2;"
    )

    node2.query(
        f"INSERT INTO FUNCTION url('http://{node1.ip_address}:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);"
    )

    assert node1.query("SELECT count(*) FROM test_table;") == "1\n"

    node2.query(
        f"SELECT count(*) FROM url('http://{node1.ip_address}:8123/?query=SELECT+*+FROM+test_table+FORMAT+CSV', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8'));"
    ) == "2\n"
