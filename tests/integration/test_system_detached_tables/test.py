import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node_default", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_system_detached_tables():
    node.query("CREATE TABLE test_table (n Int64) ENGINE=MergeTree ORDER BY n;")
    node.query("CREATE TABLE test_table_perm (n Int64) ENGINE=MergeTree ORDER BY n;")

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

    assert "" == node.query("SELECT * FROM system.detached_tables")

    node.query("DETACH TABLE test_table")
    node.query("DETACH TABLE test_table_perm PERMANENTLY")

    querry = "SELECT database, table, is_permanently, uuid, metadata_path FROM system.detached_tables FORMAT Values"
    result = node.query(querry)
    assert (
        result
        == f"('default','test_table',0,'{test_table_uuid}','{test_table_metadata_path}'),('default','test_table_perm',1,'{test_table_perm_uuid}','{test_table_perm_metadata_path}')"
    )
    node.restart_clickhouse()

    result = node.query(querry)
    assert (
        result
        == f"('default','test_table_perm',1,'{test_table_perm_uuid}','{test_table_perm_metadata_path}')"
    )

    node.restart_clickhouse()

    result = node.query(querry)
    assert (
        result
        == f"('default','test_table_perm',1,'{test_table_perm_uuid}','{test_table_perm_metadata_path}')"
    )
