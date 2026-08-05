import re

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

main_node_1 = cluster.add_instance(
    "main_node_1",
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)
main_node_2 = cluster.add_instance(
    "main_node_2",
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)
backup_node_1 = cluster.add_instance(
    "backup_node_1",
    main_configs=["configs/backup_group.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 3},
)
backup_node_2 = cluster.add_instance(
    "backup_node_2",
    main_configs=["configs/backup_group.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 4},
)

all_nodes = [
    main_node_1,
    main_node_2,
    backup_node_1,
    backup_node_2,
]

uuid_regex = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def assert_create_query(nodes, table_name, expected):
    replace_uuid = lambda x: re.sub(uuid_regex, "uuid", x)
    query = "show create table {}".format(table_name)
    for node in nodes:
        assert_eq_with_retry(node, query, expected, get_result=replace_uuid)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_cluster_groups(started_cluster):
    for node in all_nodes:
        node.query(
            f"CREATE DATABASE cluster_groups ENGINE = Replicated('/test/cluster_groups', '{node.macros['shard']}', '{node.macros['replica']}');"
        )

    # 1. system.clusters

    cluster_query = "SELECT host_name from system.clusters WHERE cluster = 'cluster_groups' ORDER BY host_name"
    expected_main = "main_node_1\nmain_node_2\n"
    expected_backup = "backup_node_1\nbackup_node_2\n"

    for node in [main_node_1, main_node_2]:
        assert_eq_with_retry(node, cluster_query, expected_main)

    for node in [backup_node_1, backup_node_2]:
        assert_eq_with_retry(node, cluster_query, expected_backup)

    # 2. Query execution depends only on your cluster group

    backup_node_1.stop_clickhouse()
    backup_node_2.stop_clickhouse()

    # OK
    main_node_1.query(
        "CREATE TABLE cluster_groups.table_1 (d Date, k UInt64) ENGINE=ReplicatedMergeTree ORDER BY k PARTITION BY toYYYYMM(d);"
    )

    # Exception
    main_node_2.stop_clickhouse()
    settings = {"distributed_ddl_task_timeout": 5}
    assert "is not finished on 1 of 2 hosts" in main_node_1.query_and_get_error(
        "CREATE TABLE cluster_groups.table_2 (d Date, k UInt64) ENGINE=ReplicatedMergeTree ORDER BY k PARTITION BY toYYYYMM(d);",
        settings=settings,
    )

    # 3. After start both groups are synced

    backup_node_1.start_clickhouse()
    backup_node_2.start_clickhouse()
    main_node_2.start_clickhouse()

    expected_1 = "CREATE TABLE cluster_groups.table_1\\n(\\n    `d` Date,\\n    `k` UInt64\\n)\\nENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nPARTITION BY toYYYYMM(d)\\nORDER BY k\\nSETTINGS index_granularity = 8192"
    expected_2 = "CREATE TABLE cluster_groups.table_2\\n(\\n    `d` Date,\\n    `k` UInt64\\n)\\nENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nPARTITION BY toYYYYMM(d)\\nORDER BY k\\nSETTINGS index_granularity = 8192"

    for node in [backup_node_1, backup_node_2, main_node_2]:
        node.query("SYSTEM SYNC DATABASE REPLICA cluster_groups;")

    assert_create_query(all_nodes, "cluster_groups.table_1", expected_1)
    assert_create_query(all_nodes, "cluster_groups.table_2", expected_2)

    # 4. SYSTEM DROP DATABASE REPLICA
    backup_node_2.stop_clickhouse()
    backup_node_1.query(
        "SYSTEM DROP DATABASE REPLICA '1|4' FROM DATABASE cluster_groups"
    )

    assert_eq_with_retry(backup_node_1, cluster_query, "backup_node_1\n")

    main_node_2.stop_clickhouse()
    main_node_1.query("SYSTEM DROP DATABASE REPLICA '1|2' FROM DATABASE cluster_groups")

    assert_eq_with_retry(main_node_1, cluster_query, "main_node_1\n")

    # 5. Reset to the original state
    backup_node_2.start_clickhouse()
    main_node_2.start_clickhouse()
    for node in all_nodes:
        node.query("DROP DATABASE cluster_groups SYNC;")
