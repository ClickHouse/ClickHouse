import pytest

from helpers.cluster import ClickHouseCluster

from .common import check_on_cluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"node{i}",
        main_configs=[
            "config/config_secure.xml",
            "config/server.crt",
            "config/server.key",
        ],
        stay_alive=True,
        with_zookeeper=True,
    )
    for i in range(2)
]

observer = cluster.add_instance(
    "observer",
    main_configs=["config/config_secure_observer.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_secure_cluster(start_cluster):
    # Every node must advertise itself as secure on `tcp_port_secure`,
    # otherwise peers either skip it or dial the plain port with TLS.
    check_on_cluster(
        nodes + [observer],
        len(nodes),
        cluster_name="test_auto_cluster_secure",
        what="countIf(port = 9440)",
        msg="Wrong secure nodes count in cluster",
    )

    result = nodes[0].query(
        "SELECT count() FROM clusterAllReplicas('test_auto_cluster_secure', system.one)"
    )
    assert result == f"{len(nodes)}\n", result

    # The observer has no secure listener of its own, but it must still be able
    # to start, discover the secure members and query them over TLS.
    result = observer.query(
        "SELECT count() FROM clusterAllReplicas('test_auto_cluster_secure', system.one)"
    )
    assert result == f"{len(nodes)}\n", result


def test_observer_does_not_treat_cohosted_member_as_local(start_cluster):
    # A member that lives on the observer's host and listens on the default secure port
    # (here: registered directly in Keeper, nothing actually listens there). The observer
    # has no `tcp_port_secure`, so it must not consider this member to be itself: otherwise
    # distributed reads and inserts are silently routed to the observer's own tables.
    zk = cluster.get_kazoo_client("zoo1")
    fake_member = "/clickhouse/discovery/test_auto_cluster_secure/shards/00000000-0000-0000-0000-00000000c0de"
    zk.create(
        fake_member,
        b'{"version":1,"address":"observer:9440","secure":true,"shard_id":0}',
    )
    try:
        check_on_cluster(
            [observer],
            len(nodes) + 1,
            cluster_name="test_auto_cluster_secure",
            what="count()",
            msg="Observer did not discover the co-hosted member",
        )

        result = observer.query(
            "SELECT countIf(is_local) FROM system.clusters WHERE cluster = 'test_auto_cluster_secure'"
        )
        assert result == "0\n", result

        result = observer.query(
            "SELECT hostName() AS h FROM clusterAllReplicas('test_auto_cluster_secure', system.one) ORDER BY h",
            settings={"skip_unavailable_shards": 1},
        )
        assert result == "".join(f"{node.name}\n" for node in nodes), result
    finally:
        zk.delete(fake_member)
        zk.stop()
        zk.close()
