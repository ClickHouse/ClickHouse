import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import check_flags_deleted, set_convert_flags

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

database_name = "modify_engine_on_ordinary"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query):
    return node.query(database=database_name, sql=query)


def create_tables():
    q(
        ch1,
        "CREATE TABLE mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )


def check_tables():
    # Check tables exists
    assert (
        q(
            ch1,
            "SHOW TABLES",
        ).strip()
        == "mt"
    )

    # Check engines
    assert (
        q(
            ch1,
            f"SELECT name, engine FROM system.tables WHERE database = '{database_name}'",
        ).strip()
        == f"mt\tMergeTree"
    )


def remove_convert_flags():
    ch1.exec_in_container(
        [
            "bash",
            "-c",
            f"rm /var/lib/clickhouse/data/{database_name}/mt/convert_to_replicated",
        ]
    )


def test_modify_engine_on_restart_ordinary_database(started_cluster):
    ch1.query(
        sql=f"CREATE DATABASE {database_name} ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )

    create_tables()

    check_tables()

    set_convert_flags(ch1, database_name, ["mt"])

    cannot_start = False
    try:
        ch1.restart_clickhouse()
    except:
        cannot_start = True
    assert cannot_start

    remove_convert_flags()

    ch1.restart_clickhouse()

    check_tables()
