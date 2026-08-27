import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "replica1",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    macros={"replica": "replica1"},
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "db_name,db_engine,table_engine,table_engine_params",
    [
        pytest.param(
            "test_db_atomic",
            "Atomic",
            "MergeTree",
            "ORDER BY n",
            id="Atomic db with MergeTree table",
        ),
        pytest.param(
            "test_db_lazy",
            "Lazy(60)",
            "Log",
            "",
            id="Lazy db with Log table",
        ),
        pytest.param(
            "test_db_repl",
            "Replicated('/clickhouse/tables/test_table','shard1', 'replica1')",
            "ReplicatedMergeTree",
            "ORDER BY n",
            id="Replicated db with ReplicatedMergeTree table",
        ),
    ],
)
def test_system_detached_tables(
    start_cluster, db_name, db_engine, table_engine, table_engine_params
):
    node.query(f"CREATE DATABASE IF NOT EXISTS {db_name} ENGINE={db_engine};")

    node.query(
        f"CREATE TABLE {db_name}.test_table (n Int64) ENGINE={table_engine} {table_engine_params};"
    )
    node.query(
        f"CREATE TABLE {db_name}.test_table_perm (n Int64) ENGINE={table_engine} {table_engine_params};"
    )

    test_table_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE table='test_table'"
    ).rstrip("\n")
    test_table_metadata_path = node.query(
        "SELECT metadata_path FROM system.tables WHERE table='test_table'"
    ).rstrip("\n")

    test_table_perm_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE table='test_table_perm'"
    ).rstrip("\n")
    test_table_perm_metadata_path = node.query(
        "SELECT metadata_path FROM system.tables WHERE table='test_table_perm'"
    ).rstrip("\n")

    assert "" == node.query(
        f"SELECT * FROM system.detached_tables WHERE database='{db_name}'"
    )

    node.query(
        f"SET database_replicated_always_detach_permanently=1; DETACH TABLE {db_name}.test_table"
    )
    node.query(f"DETACH TABLE {db_name}.test_table_perm PERMANENTLY")

    querry = f"SELECT database, table, is_permanently, uuid, metadata_path FROM system.detached_tables WHERE database='{db_name}' FORMAT Values"
    result = node.query(querry)

    if db_engine.startswith("Repl"):
        expected_before_restart = f"('{db_name}','test_table',1,'{test_table_uuid}','{test_table_metadata_path}'),('{db_name}','test_table_perm',1,'{test_table_perm_uuid}','{test_table_perm_metadata_path}')"
    else:
        expected_before_restart = f"('{db_name}','test_table',0,'{test_table_uuid}','{test_table_metadata_path}'),('{db_name}','test_table_perm',1,'{test_table_perm_uuid}','{test_table_perm_metadata_path}')"

    assert result == expected_before_restart

    if db_engine.startswith("Lazy"):
        return

    node.restart_clickhouse()

    if db_engine.startswith("Repl"):
        expected_after_restart = expected_before_restart
    else:
        expected_after_restart = f"('{db_name}','test_table_perm',1,'{test_table_perm_uuid}','{test_table_perm_metadata_path}')"

    result = node.query(querry)
    assert result == expected_after_restart

    node.restart_clickhouse()

    result = node.query(querry)
    assert result == expected_after_restart

    node.query(f"DROP DATABASE {db_name}")
