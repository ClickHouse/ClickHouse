import pytest
import os
from helpers.cluster import ClickHouseCluster
from pathlib import Path
from textwrap import dedent

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    'node',
    # Only MaterializedPostgreSQL database engine support alter, so postgres is required.
    main_configs=["configs/named_collections.xml"],
    with_postgres=True,
    stay_alive=True
)
metadata_dir = Path(node.path) / "database/store"


def prepare_postgres():
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    conn_string = f"host={cluster.postgres_ip} port={cluster.postgres_port} user='postgres' password='mysecretpassword'"
    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True

    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id Integer NOT NULL, value Integer, PRIMARY KEY (id))")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start(destroy_dirs=True)

        prepare_postgres()
        yield cluster

    finally:
        cluster.shutdown()


def test_create_table():
    db_name = "test_create"
    tb_name = "test_create"
    node.query(f"CREATE DATABASE {db_name}")
    node.query(f"CREATE TABLE {db_name}.{tb_name} (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a" )
    db_uuid = node.query(f"SELECT uuid FROM system.databases WHERE name = '{db_name}'").strip()
    tb_uuid = node.query(f"SELECT uuid FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}'").strip()

    assert tb_uuid != ""
    with open(metadata_dir / db_uuid[:3]/ db_uuid / f"{tb_name}.sql") as f:
        # Table name should be _ if database has uuid
        assert f.read() == dedent(f"""\
            ATTACH TABLE _ UUID '{tb_uuid}'
            (
                `a` UInt32,
                `b` UInt32
            )
            ENGINE = MergeTree
            ORDER BY a
            SETTINGS index_granularity = 8192
        """)

def test_drop_table():
    db_name = "test_drop"
    tb_name = "test_drop"
    node.query(f"CREATE DATABASE {db_name}")
    node.query(f"CREATE TABLE {db_name}.{tb_name} (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a" )
    node.query(f"DROP TABLE {db_name}.{tb_name}" )

    db_uuid = node.query(f"SELECT uuid FROM system.databases WHERE name = '{db_name}'").strip()
    assert node.query(f"SELECT count() FROM system.tables WHERE database = '{db_name}' and name = '{tb_name}'").strip() == "0"
    assert not os.path.exists(metadata_dir / db_uuid[:3] / db_uuid / f"{tb_name}.sql")

