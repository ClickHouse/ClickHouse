import pytest
import time

from dataclasses import dataclass
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

all_nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@dataclass(frozen=True)
class Entity:
    keyword: str
    name: str
    options: str = ""


entities = [
    Entity(keyword="USER", name="theuser"),
    Entity(keyword="ROLE", name="therole"),
    Entity(keyword="ROW POLICY", name="thepolicy", options=" ON default.t1"),
    Entity(keyword="QUOTA", name="thequota"),
    Entity(keyword="SETTINGS PROFILE", name="theprofile"),
]


def get_entity_id(entity):
    return entity.keyword.replace(" ", "_")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_create_replicated(started_cluster, entity):
    node1.query(f"CREATE {entity.keyword} {entity.name} {entity.options}")
    assert (
        f"cannot insert because {entity.keyword.lower()} `{entity.name}{entity.options}` already exists in replicated"
        in node2.query_and_get_error(
            f"CREATE {entity.keyword} {entity.name} {entity.options}"
        )
    )
    node1.query(f"DROP {entity.keyword} {entity.name} {entity.options}")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_create_and_delete_replicated(started_cluster, entity):
    node1.query(f"CREATE {entity.keyword} {entity.name} {entity.options}")
    node2.query(f"DROP {entity.keyword} {entity.name} {entity.options}")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_create_replicated_on_cluster(started_cluster, entity):
    assert (
        f"cannot insert because {entity.keyword.lower()} `{entity.name}{entity.options}` already exists in replicated"
        in node1.query_and_get_error(
            f"CREATE {entity.keyword} {entity.name} ON CLUSTER default {entity.options}"
        )
    )
    node1.query(f"DROP {entity.keyword} {entity.name} {entity.options}")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_create_replicated_if_not_exists_on_cluster(started_cluster, entity):
    node1.query(
        f"CREATE {entity.keyword} IF NOT EXISTS {entity.name} ON CLUSTER default {entity.options}"
    )
    node1.query(f"DROP {entity.keyword} {entity.name} {entity.options}")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_rename_replicated(started_cluster, entity):
    node1.query(f"CREATE {entity.keyword} {entity.name} {entity.options}")
    node2.query(
        f"ALTER {entity.keyword} {entity.name} {entity.options} RENAME TO {entity.name}2"
    )
    node1.query(f"DROP {entity.keyword} {entity.name}2 {entity.options}")


# ReplicatedAccessStorage must be able to continue working after reloading ZooKeeper.
def test_reload_zookeeper(started_cluster):
    def wait_zookeeper_node_to_start(zk_nodes, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                for instance in zk_nodes:
                    conn = cluster.get_kazoo_client(instance)
                    conn.get_children("/")
                print("All instances of ZooKeeper started")
                return
            except Exception as ex:
                print(("Can't connect to ZooKeeper " + str(ex)))
                time.sleep(0.5)

    def replace_zookeeper_config(new_config):
        node1.replace_config("/etc/clickhouse-server/conf.d/zookeeper.xml", new_config)
        node2.replace_config("/etc/clickhouse-server/conf.d/zookeeper.xml", new_config)
        node1.query("SYSTEM RELOAD CONFIG")
        node2.query("SYSTEM RELOAD CONFIG")

    def get_active_zk_connections():
        return str(
            node1.exec_in_container(
                [
                    "bash",
                    "-c",
                    "lsof -a -i4 -i6 -itcp -w | grep 2181 | grep ESTABLISHED | wc -l",
                ],
                privileged=True,
                user="root",
            )
        ).strip()

    node1.query("CREATE USER u1")
    assert_eq_with_retry(
        node2, "SELECT name FROM system.users WHERE name ='u1'", "u1\n"
    )

    ## remove zoo2, zoo3 from configs
    replace_zookeeper_config(
        """
<clickhouse>
    <zookeeper>
        <node index="1">
            <host>zoo1</host>
            <port>2181</port>
        </node>
        <session_timeout_ms>2000</session_timeout_ms>
    </zookeeper>
</clickhouse>
"""
    )

    ## config reloads, but can still work
    node1.query("CREATE USER u2")
    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2'] ORDER BY name",
        TSV(["u1", "u2"]),
    )

    ## stop all zookeepers, users will be readonly
    cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    assert node2.query(
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2'] ORDER BY name"
    ) == TSV(["u1", "u2"])
    assert "ZooKeeper" in node1.query_and_get_error("CREATE USER u3")

    ## start zoo2, zoo3, users will be readonly too, because it only connect to zoo1
    cluster.start_zookeeper_nodes(["zoo2", "zoo3"])
    wait_zookeeper_node_to_start(["zoo2", "zoo3"])
    assert node2.query(
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2'] ORDER BY name"
    ) == TSV(["u1", "u2"])
    assert "ZooKeeper" in node1.query_and_get_error("CREATE USER u3")

    ## set config to zoo2, server will be normal
    replace_zookeeper_config(
        """
<clickhouse>
    <zookeeper>
        <node index="1">
            <host>zoo2</host>
            <port>2181</port>
        </node>
        <session_timeout_ms>2000</session_timeout_ms>
    </zookeeper>
</clickhouse>
"""
    )

    active_zk_connections = get_active_zk_connections()
    assert (
        active_zk_connections == "1"
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)

    node1.query("CREATE USER u3")
    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2', 'u3'] ORDER BY name",
        TSV(["u1", "u2", "u3"]),
    )

    active_zk_connections = get_active_zk_connections()
    assert (
        active_zk_connections == "1"
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)
