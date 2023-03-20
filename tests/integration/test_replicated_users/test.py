import pytest

from dataclasses import dataclass
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/config.xml"], with_zookeeper=True, stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/config.xml"], with_zookeeper=True, stay_alive=True
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
    return entity.keyword


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
