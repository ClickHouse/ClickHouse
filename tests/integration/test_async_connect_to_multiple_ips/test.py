import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def cluster_without_dns_cache_update():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()
        pass


node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/listen_host.xml"],
    user_configs=["configs/enable_hedged.xml"],
    with_zookeeper=True,
    ipv4_address="10.5.95.11",
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/listen_host.xml"],
    user_configs=["configs/enable_hedged.xml"],
    with_zookeeper=True,
    ipv4_address="10.5.95.12",
)


# node1 - source with table, have invalid ipv6
# node2 - destination, doing remote query
def test(cluster_without_dns_cache_update):
    node1.query(
        "CREATE TABLE test(t Date, label UInt8) ENGINE = MergeTree PARTITION BY t ORDER BY label;"
    )
    node1.query("INSERT INTO test SELECT toDate('2022-12-28'), 1;")
    assert node1.query("SELECT count(*) FROM test") == "1\n"

    wrong_ip = "2001:3984:3989::1:1118"

    node2.exec_in_container(
        (["bash", "-c", "echo '{} {}' >> /etc/hosts".format(wrong_ip, node1.name)])
    )
    node2.exec_in_container(
        (
            [
                "bash",
                "-c",
                "echo '{} {}' >> /etc/hosts".format(node1.ipv4_address, node1.name),
            ]
        )
    )

    assert node1.query("SELECT count(*) from test") == "1\n"
    node2.query("SYSTEM DROP DNS CACHE")
    node1.query("SYSTEM DROP DNS CACHE")
    assert (
        node2.query(
            f"SELECT count(*) FROM remote('{node1.name}', default.test) limit 1;"
        )
        == "1\n"
    )
