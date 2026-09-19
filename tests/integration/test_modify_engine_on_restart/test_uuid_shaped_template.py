import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import get_table_path, set_convert_flags

cluster = ClickHouseCluster(__file__)

# A table of an Ordinary database stores its expanded ZooKeeper path literally, and a later load recovers the
# znode it owns from the last UUID-shaped substring of that path. A UUID-shaped {shard} value would come after
# the generated UUID and take that place, so the conversion must be refused instead of leaking the parent znode.
SHARD_UUID = "123e4567-e89b-12d3-a456-426614174111"
ch_shard = cluster.add_instance(
    "ch_shard",
    main_configs=["configs/config.d/convert_shard_uuid.xml"],
    with_zookeeper=True,
    macros={"shard": SHARD_UUID, "replica": "node1"},
    stay_alive=True,
)
# `default_replica_name` is stored as a template even for Ordinary databases, so a {uuid} in it must be refused
# before the metadata is rewritten; otherwise the converted table could never be attached again.
ch_replica_name = cluster.add_instance(
    "ch_replica_name",
    main_configs=["configs/config.d/convert_replica_name_uuid.xml"],
    with_zookeeper=True,
    macros={"shard": "01", "replica": "node1"},
)

database_name = "modify_engine_uuid_shaped"


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


def check_attach_as_replicated_refused(node, table, expected_error):
    q(node, f"DETACH TABLE {table}")
    assert expected_error in node.query_and_get_error(
        database=database_name, sql=f"ATTACH TABLE {table} AS REPLICATED"
    )
    # The refusal happened before the metadata was rewritten: the table attaches again as it was.
    q(node, f"ATTACH TABLE {table}")
    assert get_engine(node, table) == "MergeTree"
    assert q(node, f"SELECT count() FROM {table}").strip() == "1"


def test_uuid_shaped_shard_refused_for_ordinary(started_cluster):
    create_database(ch_shard, "Ordinary")
    create_mergetree_table(ch_shard, "mt")
    check_attach_as_replicated_refused(
        ch_shard,
        "mt",
        "another UUID-shaped path component after the one expanded from the {uuid} macro",
    )
    ch_shard.query(f"DROP DATABASE {database_name} SYNC")


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
        q(
            ch_shard,
            f"SELECT zookeeper_path FROM system.replicas WHERE database = '{database_name}' AND table = 'mt'",
        ).strip()
        == f"/clickhouse/tables/{uuid}/{SHARD_UUID}"
    )
    q(ch_shard, "SYSTEM RESTORE REPLICA mt")
    assert q(ch_shard, "SELECT count() FROM mt").strip() == "1"
    q(ch_shard, "DROP TABLE mt SYNC")
    # The parent znode is owned by the table and goes away with it.
    assert (
        ch_shard.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables' AND name = '{uuid}'"
        ).strip()
        == "0"
    )
    ch_shard.query(f"DROP DATABASE {database_name} SYNC")


def test_uuid_shaped_shard_refused_on_restart_for_ordinary(started_cluster):
    create_database(ch_shard, "Ordinary")
    create_mergetree_table(ch_shard, "flagged")
    set_convert_flags(ch_shard, database_name, ["flagged"])
    table_data_path = get_table_path(ch_shard, "flagged", database_name)

    ch_shard.stop_clickhouse()
    ch_shard.start_clickhouse(start_wait_sec=120, expected_to_fail=True)
    assert ch_shard.contains_in_log(
        "another UUID-shaped path component after the one expanded from the {uuid} macro"
    )

    # Cancelling the conversion lets the server start again with the table still unconverted.
    ch_shard.exec_in_container(
        ["bash", "-c", f"rm {table_data_path}convert_to_replicated"]
    )
    ch_shard.start_clickhouse()
    assert get_engine(ch_shard, "flagged") == "MergeTree"
    assert q(ch_shard, "SELECT count() FROM flagged").strip() == "1"
    ch_shard.query(f"DROP DATABASE {database_name} SYNC")


@pytest.mark.parametrize("engine", ["Atomic", "Ordinary"])
def test_uuid_in_replica_name_refused(started_cluster, engine):
    create_database(ch_replica_name, engine)
    create_mergetree_table(ch_replica_name, "mt")
    check_attach_as_replicated_refused(
        ch_replica_name, "mt", "Macro 'uuid' in engine arguments is only supported"
    )
    ch_replica_name.query(f"DROP DATABASE {database_name} SYNC")
