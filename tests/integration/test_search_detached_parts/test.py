import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
from helpers.network import PartitionManager

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/storage_conf.xml"],
    user_configs=["configs/users.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_search_detached_parts():
    table_name = 't1'

    for storage_policy in ["no_s3", "local_cache"]:
        search_mode='any'
        node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
        node1.query(
            f"""
            CREATE TABLE {table_name} (
            id Int64,
            data String
            ) ENGINE=MergeTree()
            PARTITION BY id % 10
            ORDER BY id
            SETTINGS storage_policy='{storage_policy}', search_detached_parts_drives='{search_mode}'
            """)
        node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

        search_mode='none'
        node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
        node1.query(
            f"""
            CREATE TABLE {table_name} (
            id Int64,
            data String
            ) ENGINE=MergeTree()
            PARTITION BY id % 10
            ORDER BY id
            SETTINGS storage_policy='{storage_policy}', search_detached_parts_drives='{search_mode}'
            """)
        # To drop when minio is not available


        with PartitionManager() as pm:
            isolation_rules=[{
                "source": node1.ip_address,
                "destination": cluster.get_instance_ip("minio1"),
                "action": "REJECT --reject-with tcp-reset",
            }]
            pm.push_rules(isolation_rules)

            search_mode='none'
            node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
            node1.query(
                f"""
                CREATE TABLE {table_name} (
                id Int64,
                data String
                ) ENGINE=MergeTree()
                PARTITION BY id % 10
                ORDER BY id
                SETTINGS storage_policy='{storage_policy}', search_detached_parts_drives='{search_mode}'
                """)
            node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

            search_mode='local'
            node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
            node1.query(
                f"""
                CREATE TABLE {table_name} (
                id Int64,
                data String
                ) ENGINE=MergeTree()
                PARTITION BY id % 10
                ORDER BY id
                SETTINGS storage_policy='{storage_policy}', search_detached_parts_drives='{search_mode}'
                """)
            node1.query(f"ALTER TABLE {table_name} ADD COLUMN nc Int32 SETTINGS alter_sync = 1")
            node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

            search_mode='any'
            node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
            assert "Code: 499. DB::Exception" in node1.query_and_get_error(
                f"""
                CREATE TABLE {table_name} (
                id Int64,
                data String
                ) ENGINE=MergeTree()
                PARTITION BY id % 10
                ORDER BY id
                SETTINGS storage_policy='{storage_policy}', search_detached_parts_drives='{search_mode}'
                """)
            node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
