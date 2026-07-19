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

database_name = "modify_engine_unique_key"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query, **kwargs):
    return node.query(database=database_name, sql=query, **kwargs)


def test_convert_to_replicated_rejected_for_unique_key(started_cluster):
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    q(
        ch1,
        "CREATE TABLE uk ( A Int64 ) ENGINE = MergeTree() ORDER BY tuple() UNIQUE KEY (A)",
        settings={"allow_experimental_unique_key": 1},
    )
    q(ch1, "INSERT INTO uk VALUES (1), (2)")

    set_convert_flags(ch1, database_name, ["uk"])

    table_data_path = get_table_path(ch1, "uk", database_name)

    ch1.stop_clickhouse()
    ch1.start_clickhouse(start_wait_sec=120, expected_to_fail=True)

    ch1.exec_in_container(["rm", f"{table_data_path}convert_to_replicated"])
    ch1.start_clickhouse()

    assert (
        q(ch1, "SELECT engine FROM system.tables WHERE name = 'uk'").strip()
        == "MergeTree"
    )
    assert q(ch1, "SELECT count() FROM uk").strip() == "2"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
