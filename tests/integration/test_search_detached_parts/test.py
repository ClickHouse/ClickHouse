import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
from helpers.network import PartitionManager

node1 = cluster.add_instance(
    "node1",
    # main_configs=["configs/storage_conf.xml", "configs/users.xml"],
    main_configs=["configs/storage_conf.xml",],
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    # cluster.add_instance(
    #     "node1",
    #     main_configs=["configs/storage_conf.xml"],
    #     with_minio=True,
    #     stay_alive=True,
    # )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_search_detached_parts():
    table_name = 't1'
    storage_policy = 's3_plain_rewritable'
    # node = cluster.instances["node1"]
    node1.query(f"SET search_detached_parts_drives='none'; DROP TABLE IF EXISTS {table_name} SYNC")
    node1.query(
        f"""
        SET search_detached_parts_drives='any';
        CREATE TABLE {table_name} (
        id Int64,
        data String
        ) ENGINE=MergeTree()
        PARTITION BY id % 10
        ORDER BY id
        SETTINGS storage_policy='{storage_policy}'
        """)

    with PartitionManager() as pm:
        pm.push_rules([{
                "source": node1.ip_address,
                "destination": cluster.get_instance_ip("minio1"),
                # "action": "DROP"
                "action": "REJECT --reject-with tcp-reset",
            }])
        node1.query(f"SET search_detached_parts_drives='none'; DROP TABLE IF EXISTS {table_name} SYNC")
        node1.query(
            f"""
            SET s3_connect_timeout_ms=1000;
            SET enable_s3_requests_logging=true;
            SET search_detached_parts_drives='any';
            CREATE TABLE {table_name} (
            id Int64,
            data String
            ) ENGINE=MergeTree()
            PARTITION BY id % 10
            ORDER BY id
            SETTINGS storage_policy='{storage_policy}'
            """)

    # with cluster.pause_container("minio1"):
    #     node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
    #     node1.query(
    #         f"""
    #         CREATE TABLE {table_name} (
    #         id Int64,
    #         data String
    #         ) ENGINE=MergeTree()
    #         PARTITION BY id % 10
    #         ORDER BY id
    #         SETTINGS storage_policy='{storage_policy}'
    #         """)
