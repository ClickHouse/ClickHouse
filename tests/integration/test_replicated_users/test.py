import inspect
from dataclasses import dataclass
from os import path as p

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import (
    get_active_zk_connections,
    replace_zookeeper_config,
    reset_zookeeper_config,
)
from helpers.test_tools import TSV, assert_eq_with_retry

default_zk_config = p.join(p.dirname(p.realpath(__file__)), "configs/zookeeper.xml")
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

# A node in a different time zone; used only by `test_valid_for_replicated_mixed_timezone`
# and deliberately kept out of `all_nodes`.
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/config.xml", "configs/timezone.xml"],
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
        f"cannot insert because {entity.keyword.lower()} `{entity.name}{entity.options}` already exists in `replicated`"
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
        f"cannot insert because {entity.keyword.lower()} `{entity.name}{entity.options}` already exists in `replicated`"
        in node1.query_and_get_error(
            f"CREATE {entity.keyword} {entity.name} ON CLUSTER default {entity.options}"
        )
    )
    node1.query(f"DROP {entity.keyword} {entity.name} {entity.options}")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_create_replicated_on_cluster_ignore(started_cluster, entity):
    node1.replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        inspect.cleandoc(
            """
            <clickhouse>
                <profiles>
                    <default>
                        <ignore_on_cluster_for_replicated_access_entities_queries>true</ignore_on_cluster_for_replicated_access_entities_queries>
                    </default>
                </profiles>
            </clickhouse>
            """
        ),
    )
    node1.query("SYSTEM RELOAD CONFIG")

    node1.query(
        f"CREATE {entity.keyword} {entity.name} ON CLUSTER default {entity.options}"
    )
    assert (
        f"cannot insert because {entity.keyword.lower()} `{entity.name}{entity.options}` already exists in `replicated`"
        in node2.query_and_get_error_with_retry(
            f"CREATE {entity.keyword} {entity.name} {entity.options}"
        )
    )

    node1.query(f"DROP {entity.keyword} {entity.name} {entity.options}")

    node1.replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        inspect.cleandoc(
            """
            <clickhouse>
                <profiles>
                    <default/>
                </profiles>
            </clickhouse>
            """
        ),
    )
    node1.query("SYSTEM RELOAD CONFIG")


@pytest.mark.parametrize(
    "use_on_cluster",
    [
        pytest.param(False, id="Without_on_cluster"),
        pytest.param(True, id="With_ignored_on_cluster"),
    ],
)
def test_grant_revoke_replicated(started_cluster, use_on_cluster: bool):
    node1.replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        inspect.cleandoc(
            f"""
            <clickhouse>
                <profiles>
                    <default>
                        <ignore_on_cluster_for_replicated_access_entities_queries>{int(use_on_cluster)}</ignore_on_cluster_for_replicated_access_entities_queries>
                    </default>
                </profiles>
            </clickhouse>
            """
        ),
    )
    node1.query("SYSTEM RELOAD CONFIG")
    on_cluster = "ON CLUSTER default" if use_on_cluster else ""

    node1.query(f"CREATE USER theuser2 {on_cluster}")

    assert node1.query(f"GRANT {on_cluster} SELECT ON *.* to theuser2") == ""

    assert node2.query("SHOW GRANTS FOR theuser2") == "GRANT SELECT ON *.* TO theuser2\n"

    assert node1.query(f"REVOKE {on_cluster} SELECT ON *.* from theuser2") == ""
    node1.query(f"DROP USER theuser2 {on_cluster}")

    node1.replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        inspect.cleandoc(
            """
            <clickhouse>
                <profiles>
                    <default/>
                </profiles>
            </clickhouse>
            """
        ),
    )
    node1.query("SYSTEM RELOAD CONFIG")



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
def test_valid_for_replicated_mixed_timezone(started_cluster):
    # `node3` runs in a different time zone than `node1`/`node2`. A deadline coming from
    # `VALID FOR <interval>` (or `VALID UNTIL`) must denote the same instant on every replica:
    # the entity is replicated through ZooKeeper as an `ATTACH USER ... VALID UNTIL '...'` string,
    # which is serialized with an explicit `UTC` suffix, so a replica in another time zone parses
    # it back to the same epoch instead of reinterpreting a bare local-time string in its own zone.
    assert node3.query("SELECT timezone()") != node1.query("SELECT timezone()")

    # Use the mode from the report: `ON CLUSTER` is stripped by
    # `ignore_on_cluster_for_replicated_access_entities_queries`, the query executes locally and
    # the entity propagates through the replicated access storage.
    node3.replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        inspect.cleandoc(
            """
            <clickhouse>
                <profiles>
                    <default>
                        <ignore_on_cluster_for_replicated_access_entities_queries>true</ignore_on_cluster_for_replicated_access_entities_queries>
                    </default>
                </profiles>
            </clickhouse>
            """
        ),
    )
    node3.query("SYSTEM RELOAD CONFIG")

    node3.query("DROP USER IF EXISTS user_valid_for_tz")
    node3.query(
        "CREATE USER user_valid_for_tz ON CLUSTER default VALID FOR INTERVAL 30 DAY"
    )

    epoch_query = "SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_valid_for_tz'"
    expected = node3.query(epoch_query).strip()
    assert expected != "0"
    assert_eq_with_retry(node1, epoch_query, expected)
    assert_eq_with_retry(node2, epoch_query, expected)

    # `ALTER` takes the same replicated serialization path.
    node3.query("ALTER USER user_valid_for_tz VALID FOR INTERVAL 60 DAY")
    expected = node3.query(epoch_query).strip()
    assert_eq_with_retry(node1, epoch_query, expected)
    assert_eq_with_retry(node2, epoch_query, expected)

    node3.query("DROP USER user_valid_for_tz")
    node3.replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        inspect.cleandoc(
            """
            <clickhouse>
                <profiles>
                    <default/>
                </profiles>
            </clickhouse>
            """
        ),
    )
    node3.query("SYSTEM RELOAD CONFIG")


def test_reload_zookeeper(started_cluster):
    node1.query("CREATE USER u1")
    assert_eq_with_retry(
        node2, "SELECT name FROM system.users WHERE name ='u1'", "u1\n"
    )

    ## remove zoo2, zoo3 from configs
    replace_zookeeper_config(
        (node1, node2),
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
""",
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
    cluster.wait_zookeeper_nodes_to_start(["zoo2", "zoo3"])
    assert node2.query(
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2'] ORDER BY name"
    ) == TSV(["u1", "u2"])
    assert "ZooKeeper" in node1.query_and_get_error("CREATE USER u3")

    ## set config to zoo2, server will be normal
    replace_zookeeper_config(
        (node1, node2),
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
""",
    )

    active_zk_connections = get_active_zk_connections(node1)
    assert (
        len(active_zk_connections) == 1
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)

    node1.query("CREATE USER u3")
    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2', 'u3'] ORDER BY name",
        TSV(["u1", "u2", "u3"]),
    )

    active_zk_connections = get_active_zk_connections(node1)
    assert (
        len(active_zk_connections) == 1
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)

    # Restore the test state
    node1.query("DROP USER u1, u2, u3")
    cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    cluster.wait_zookeeper_nodes_to_start(["zoo1", "zoo2", "zoo3"])
    reset_zookeeper_config((node1, node2), default_zk_config)
