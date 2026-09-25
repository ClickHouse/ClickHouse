import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import get_table_path, set_convert_flags

cluster = ClickHouseCluster(__file__)

# Every node below needs its own {shard}/{replica} pair: with a database disk the root of the metadata storage
# is named after those macros, so two nodes sharing them would share one metadata directory as well.

# A table of an Ordinary database stores its expanded ZooKeeper path literally, and a later load recovers the
# znode it owns by matching that path against `default_replica_path` again. The boundary therefore comes from
# the template, so a UUID-shaped {shard} value standing right after the generated UUID changes nothing.
SHARD_UUID = "123e4567-e89b-12d3-a456-426614174111"
ch_shard = cluster.add_instance(
    "ch_shard",
    main_configs=["configs/config.d/convert_shard_uuid.xml"],
    with_zookeeper=True,
    macros={"shard": SHARD_UUID, "replica": "node1"},
    stay_alive=True,
)
# The {uuid} macro may sit inside a path component, next to other text.
ch_inside_component = cluster.add_instance(
    "ch_inside_component",
    main_configs=["configs/config.d/convert_uuid_inside_component.xml"],
    with_zookeeper=True,
    macros={"shard": "01", "replica": "node2"},
)
# A template that expands {uuid} more than once cannot be matched back against the literal path: the table
# would not know which znode it owns, so the conversion must be refused instead of leaking the parent znode.
ch_two_uuids = cluster.add_instance(
    "ch_two_uuids",
    main_configs=["configs/config.d/convert_two_uuids.xml"],
    with_zookeeper=True,
    macros={"shard": "01", "replica": "node3"},
    stay_alive=True,
)
# `default_replica_name` is stored as a template even for Ordinary databases, so a {uuid} in it must be refused
# before the metadata is rewritten; otherwise the converted table could never be attached again.
ch_replica_name = cluster.add_instance(
    "ch_replica_name",
    main_configs=["configs/config.d/convert_replica_name_uuid.xml"],
    with_zookeeper=True,
    macros={"shard": "01", "replica": "node4"},
)

# A template that describes the position of {uuid} through the name of the table cannot survive a rename: the
# literal path keeps the old name, so a later load would no longer find the znode the conversion minted.
ch_name_in_path = cluster.add_instance(
    "ch_name_in_path",
    main_configs=["configs/config.d/convert_name_in_path.xml"],
    with_zookeeper=True,
    macros={"shard": "01", "replica": "node5"},
)

database_name = "modify_engine_uuid_shaped"

CANNOT_MATCH_ERROR = "cannot be matched back against the default_replica_path template"
NAME_IN_PATH_ERROR = "is located inside the default_replica_path template"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query, settings=None):
    return node.query(database=database_name, sql=query, settings=settings)


def create_database(node, engine):
    node.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    node.query(
        sql=f"CREATE DATABASE {database_name} ENGINE = {engine}",
        settings={"allow_deprecated_database_ordinary": 1},
    )


def create_mergetree_table(node, table):
    q(
        node,
        f"CREATE TABLE {table} ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A",
    )
    q(node, f"INSERT INTO {table} VALUES (1, '2024-01-01', 'a')")


def get_engine(node, table):
    return q(
        node,
        f"SELECT engine FROM system.tables WHERE database = '{database_name}' AND table = '{table}'",
    ).strip()


def get_zookeeper_path(node, table):
    return q(
        node,
        f"SELECT zookeeper_path FROM system.replicas WHERE database = '{database_name}' AND table = '{table}'",
    ).strip()


def znode_exists(node, path, name):
    return (
        node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{path}' AND name = '{name}'"
        ).strip()
        == "1"
    )


def check_attach_as_replicated_refused(node, table, expected_error):
    q(node, f"DETACH TABLE {table}")
    assert expected_error in node.query_and_get_error(
        database=database_name, sql=f"ATTACH TABLE {table} AS REPLICATED"
    )
    # The refusal happened before the metadata was rewritten: the table attaches again as it was.
    q(node, f"ATTACH TABLE {table}")
    assert get_engine(node, table) == "MergeTree"
    assert q(node, f"SELECT count() FROM {table}").strip() == "1"


def check_conversion_owns_parent_znode(node, expected_last_component):
    """Convert an Ordinary table, then check that DROP TABLE takes the minted parent znode with it."""
    create_database(node, "Ordinary")
    create_mergetree_table(node, "mt")
    q(node, "DETACH TABLE mt")
    q(node, "ATTACH TABLE mt AS REPLICATED")
    assert get_engine(node, "mt") == "ReplicatedMergeTree"

    zookeeper_path = get_zookeeper_path(node, "mt")
    parent_path, _, last_component = zookeeper_path.rpartition("/")
    grandparent_path, _, parent_name = parent_path.rpartition("/")
    assert last_component == expected_last_component
    assert grandparent_path == "/clickhouse/tables"

    # The conversion only rewrites the metadata; the znodes appear once the replica is restored.
    q(node, "SYSTEM RESTORE REPLICA mt")
    assert q(node, "SELECT count() FROM mt").strip() == "1"

    # A round trip through a plain re-attach reloads the literal path, which is what a restart does.
    q(node, "DETACH TABLE mt")
    q(node, "ATTACH TABLE mt")
    assert znode_exists(node, grandparent_path, parent_name)

    q(node, "DROP TABLE mt SYNC")
    # The table owns the component holding the minted UUID ...
    assert not znode_exists(node, grandparent_path, parent_name)
    # ... and nothing above it.
    assert znode_exists(node, "/clickhouse", "tables")
    node.query(f"DROP DATABASE {database_name} SYNC")
    return parent_name


def test_uuid_shaped_shard_accepted_for_ordinary(started_cluster):
    check_conversion_owns_parent_znode(ch_shard, SHARD_UUID)


def test_uuid_inside_component_accepted_for_ordinary(started_cluster):
    parent_name = check_conversion_owns_parent_znode(ch_inside_component, "01")
    assert parent_name.startswith("pika") and parent_name.endswith("chu")


def test_uuid_shaped_shard_accepted_for_atomic(started_cluster):
    # An Atomic table keeps the {uuid} macro in its metadata, so its owned znode never has to be guessed.
    create_database(ch_shard, "Atomic")
    create_mergetree_table(ch_shard, "mt")
    q(ch_shard, "DETACH TABLE mt")
    q(ch_shard, "ATTACH TABLE mt AS REPLICATED")
    assert get_engine(ch_shard, "mt") == "ReplicatedMergeTree"
    uuid = q(
        ch_shard,
        f"SELECT uuid FROM system.tables WHERE database = '{database_name}' AND table = 'mt'",
    ).strip()
    assert (
        get_zookeeper_path(ch_shard, "mt") == f"/clickhouse/tables/{uuid}/{SHARD_UUID}"
    )
    q(ch_shard, "SYSTEM RESTORE REPLICA mt")
    assert q(ch_shard, "SELECT count() FROM mt").strip() == "1"
    q(ch_shard, "DROP TABLE mt SYNC")
    # The parent znode is owned by the table and goes away with it.
    assert not znode_exists(ch_shard, "/clickhouse/tables", uuid)
    ch_shard.query(f"DROP DATABASE {database_name} SYNC")


def test_two_uuids_refused_for_ordinary(started_cluster):
    create_database(ch_two_uuids, "Ordinary")
    create_mergetree_table(ch_two_uuids, "mt")
    check_attach_as_replicated_refused(ch_two_uuids, "mt", CANNOT_MATCH_ERROR)
    ch_two_uuids.query(f"DROP DATABASE {database_name} SYNC")


def test_two_uuids_accepted_for_atomic(started_cluster):
    # An Atomic table keeps the {uuid} macro in its metadata, so the template is never matched back.
    create_database(ch_two_uuids, "Atomic")
    create_mergetree_table(ch_two_uuids, "mt")
    q(ch_two_uuids, "DETACH TABLE mt")
    q(ch_two_uuids, "ATTACH TABLE mt AS REPLICATED")
    assert get_engine(ch_two_uuids, "mt") == "ReplicatedMergeTree"
    uuid = q(
        ch_two_uuids,
        f"SELECT uuid FROM system.tables WHERE database = '{database_name}' AND table = 'mt'",
    ).strip()
    assert (
        get_zookeeper_path(ch_two_uuids, "mt") == f"/clickhouse/tables/{uuid}/{uuid}/01"
    )
    ch_two_uuids.query(f"DROP DATABASE {database_name} SYNC")


def test_two_uuids_refused_on_restart_for_ordinary(started_cluster):
    create_database(ch_two_uuids, "Ordinary")
    create_mergetree_table(ch_two_uuids, "flagged")
    set_convert_flags(ch_two_uuids, database_name, ["flagged"])
    table_data_path = get_table_path(ch_two_uuids, "flagged", database_name)

    ch_two_uuids.stop_clickhouse()
    ch_two_uuids.start_clickhouse(start_wait_sec=120, expected_to_fail=True)
    assert ch_two_uuids.contains_in_log(CANNOT_MATCH_ERROR)

    # Cancelling the conversion lets the server start again with the table still unconverted.
    ch_two_uuids.exec_in_container(
        ["bash", "-c", f"rm {table_data_path}convert_to_replicated"]
    )
    ch_two_uuids.start_clickhouse()
    assert get_engine(ch_two_uuids, "flagged") == "MergeTree"
    assert q(ch_two_uuids, "SELECT count() FROM flagged").strip() == "1"
    ch_two_uuids.query(f"DROP DATABASE {database_name} SYNC")


@pytest.mark.parametrize("engine", ["Atomic", "Ordinary"])
def test_uuid_in_replica_name_refused(started_cluster, engine):
    create_database(ch_replica_name, engine)
    create_mergetree_table(ch_replica_name, "mt")
    check_attach_as_replicated_refused(
        ch_replica_name, "mt", "Macro 'uuid' in engine arguments is only supported"
    )
    ch_replica_name.query(f"DROP DATABASE {database_name} SYNC")


def test_name_in_path_refused_for_ordinary(started_cluster):
    create_database(ch_name_in_path, "Ordinary")
    create_mergetree_table(ch_name_in_path, "mt")
    check_attach_as_replicated_refused(ch_name_in_path, "mt", NAME_IN_PATH_ERROR)
    ch_name_in_path.query(f"DROP DATABASE {database_name} SYNC")


def test_name_in_path_accepted_for_atomic(started_cluster):
    # An Atomic table keeps the macros in its metadata, so a rename re-expands the template with the new name
    # and the table never has to find the minted UUID in a literal path.
    create_database(ch_name_in_path, "Atomic")
    create_mergetree_table(ch_name_in_path, "mt")
    q(ch_name_in_path, "DETACH TABLE mt")
    q(ch_name_in_path, "ATTACH TABLE mt AS REPLICATED")
    assert get_engine(ch_name_in_path, "mt") == "ReplicatedMergeTree"
    uuid = q(
        ch_name_in_path,
        f"SELECT uuid FROM system.tables WHERE database = '{database_name}' AND table = 'mt'",
    ).strip()
    assert (
        get_zookeeper_path(ch_name_in_path, "mt")
        == f"/clickhouse/tables/{database_name}/mt/{uuid}/01"
    )
    q(ch_name_in_path, "SYSTEM RESTORE REPLICA mt")
    assert q(ch_name_in_path, "SELECT count() FROM mt").strip() == "1"
    q(ch_name_in_path, "DROP TABLE mt SYNC")
    # The table owns the znode named after its UUID and nothing above it.
    assert not znode_exists(
        ch_name_in_path, f"/clickhouse/tables/{database_name}/mt", uuid
    )
    assert znode_exists(ch_name_in_path, f"/clickhouse/tables/{database_name}", "mt")
    ch_name_in_path.query(f"DROP DATABASE {database_name} SYNC")
