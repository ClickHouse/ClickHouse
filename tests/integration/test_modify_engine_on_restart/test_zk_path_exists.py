import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import get_table_path, set_convert_flags

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
)

database_name = "modify_engine_zk_path"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query):
    return node.query(database=database_name, sql=query)


def test_modify_engine_fails_if_zk_path_exists(started_cluster):
    ch1.query("CREATE DATABASE " + database_name)

    q(
        ch1,
        "CREATE TABLE already_exists_1 ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )
    uuid = q(
        ch1,
        f"SELECT uuid FROM system.tables WHERE table = 'already_exists_1' and database = '{database_name}'",
    ).strip("'[]\n")

    q(
        ch1,
        f"CREATE TABLE already_exists_2 ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('/clickhouse/tables/{uuid}/{{shard}}', 'node2') PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    set_convert_flags(ch1, database_name, ["already_exists_1"])

    table_data_path = get_table_path(ch1, "already_exists_1", database_name)

    ch1.stop_clickhouse()
    ch1.start_clickhouse(start_wait_sec=120, expected_to_fail=True)

    # Check if we can cancel convertation
    ch1.exec_in_container(
        [
            "bash",
            "-c",
            f"rm {table_data_path}convert_to_replicated",
        ]
    )
    ch1.start_clickhouse()
