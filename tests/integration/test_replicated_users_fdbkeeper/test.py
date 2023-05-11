import pytest
import time

from dataclasses import dataclass
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_foundationdb=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_foundationdb=True,
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
        in node2.query_and_get_error_with_retry(
            f"CREATE {entity.keyword} {entity.name} {entity.options}"
        )
    )
    node1.query(f"DROP {entity.keyword} {entity.name} {entity.options}")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_create_and_delete_replicated(started_cluster, entity):
    node1.query(f"CREATE {entity.keyword} {entity.name} {entity.options}")
    node2.query_with_retry(f"DROP {entity.keyword} {entity.name} {entity.options}")


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
    node2.query_with_retry(
        f"ALTER {entity.keyword} {entity.name} {entity.options} RENAME TO {entity.name}2"
    )
    node1.query("SYSTEM RELOAD USERS")
    node1.query(f"DROP {entity.keyword} {entity.name}2 {entity.options}")


# ReplicatedAccessStorage must be able to continue working after reloading ZooKeeper.
def test_reload_fdbkeeper(started_cluster):
    def replace_fdbkeeper_cluster(new_fdb_cluster):
        logging.debug(f"Use new fdb cluster: {new_fdb_cluster}")
        for node in [node1, node2]:
            node.replace_config("/tmp/fdb.cluster", new_fdb_cluster)
            node.replace_in_config("/etc/clickhouse-server/conf.d/fdb_config.xml", "\\/etc\\/foundationdb\\/fdb.cluster", "\\/tmp\\/fdb.cluster")
            node.query("SYSTEM RELOAD CONFIG")

    node1.query("CREATE USER u1")
    assert_eq_with_retry(
        node2, "SELECT name FROM system.users WHERE name ='u1'", "u1\n"
    )

    node1.query("CREATE USER u2")
    ## stop fdb, users will be readonly
    import logging
    cluster.stop_fdb()
    assert node2.query(
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2'] ORDER BY name"
    ) == TSV(["u1", "u2"])
    assert "ZooKeeper" in node1.query_and_get_error("CREATE USER u3")

    ## switch to fdb2, server will be normal
    cluster.switch_to_fdb2()
    replace_fdbkeeper_cluster(cluster.get_fdb2_cluster())
    logging.info(cluster.get_instance_ip(cluster.foundationdb_host))
    node1.query("CREATE USER u3")
    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2', 'u3'] ORDER BY name",
        TSV(["u1", "u2", "u3"]),
    )
