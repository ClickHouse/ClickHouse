import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import (
    check_flags_deleted,
    get_table_path,
    set_convert_flags,
)

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters_auxiliary_zookeeper.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
)

database_name = "modify_engine_aux_zk"


def create_aux_root(zk):
    # The auxiliary Keeper is chrooted at /aux_root, and a chroot must exist before a client connects to it.
    zk.ensure_path("/aux_root")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_zookeeper_startup_command(create_aux_root)
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query):
    return node.query(database=database_name, sql=query)


def create_database(engine):
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(
        sql=f"CREATE DATABASE {database_name} ENGINE = {engine}",
        settings={"allow_deprecated_database_ordinary": 1},
    )


def check_converted_to_auxiliary_keeper(table):
    assert (
        q(
            ch1,
            f"SELECT engine FROM system.tables WHERE database = '{database_name}' AND table = '{table}'",
        ).strip()
        == "ReplicatedMergeTree"
    )
    assert q(
        ch1,
        f"SELECT zookeeper_name, zookeeper_path FROM system.replicas WHERE database = '{database_name}' AND table = '{table}'",
    ).strip() == "\t".join(
        ["zookeeper_aux", f"/clickhouse/tables/{database_name}/{table}/01"]
    )
    # The auxiliary Keeper is chrooted, so the table's metadata is visible from the default one under /aux_root.
    assert (
        ch1.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '/aux_root/clickhouse/tables/{database_name}/{table}/01/replicas'"
        ).strip()
        == "1"
    )


@pytest.mark.parametrize("engine", ["Atomic", "Ordinary"])
def test_convert_to_replicated_in_auxiliary_zookeeper(started_cluster, engine):
    create_database(engine)
    q(
        ch1,
        "CREATE TABLE mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A",
    )
    q(ch1, "INSERT INTO mt VALUES (1, '2024-01-01', 'a')")

    set_convert_flags(ch1, database_name, ["mt"])
    ch1.restart_clickhouse()

    check_flags_deleted(ch1, database_name, ["mt"])
    check_converted_to_auxiliary_keeper("mt")
    assert q(ch1, "SELECT count() FROM mt").strip() == "1"

    ch1.query(f"DROP DATABASE {database_name} SYNC")


@pytest.mark.parametrize("engine", ["Atomic", "Ordinary"])
def test_convert_to_replicated_refused_if_path_exists_in_auxiliary_zookeeper(
    started_cluster, engine
):
    create_database(engine)
    q(
        ch1,
        "CREATE TABLE occupied ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A",
    )
    # Occupy the path the conversion would mint, but only in the auxiliary Keeper: an existence probe
    # that went to the default Keeper would not see it and let the conversion collide with this table.
    q(
        ch1,
        f"CREATE TABLE other ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('zookeeper_aux:/clickhouse/tables/{database_name}/occupied/{{shard}}', 'node2') PARTITION BY toYYYYMM(D) ORDER BY A",
    )

    set_convert_flags(ch1, database_name, ["occupied"])
    table_data_path = get_table_path(ch1, "occupied", database_name)

    ch1.stop_clickhouse()
    ch1.start_clickhouse(start_wait_sec=120, expected_to_fail=True)
    assert ch1.contains_in_log(
        f"Found existing ZooKeeper path zookeeper_aux:/clickhouse/tables/{database_name}/occupied/01 while trying to convert table"
    )

    # Cancelling the conversion lets the server start again with the table still unconverted.
    ch1.exec_in_container(["bash", "-c", f"rm {table_data_path}convert_to_replicated"])
    ch1.start_clickhouse()
    assert (
        q(
            ch1,
            f"SELECT engine FROM system.tables WHERE database = '{database_name}' AND table = 'occupied'",
        ).strip()
        == "MergeTree"
    )

    ch1.query(f"DROP DATABASE {database_name} SYNC")
