import uuid

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_creation_expression,
    get_uuid_str,
)


# Regression for https://github.com/ClickHouse/ClickHouse/issues/80031:
# `icebergS3Cluster` reported partition pruning but still read the pruned files.
# Assert on the files actually opened across the cluster (not the pruning metric,
# which was already non-zero in the bug report).
@pytest.mark.parametrize("storage_type", ["s3"])
def test_cluster_partition_pruning_reads(started_cluster_iceberg_no_spark, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    nodes = list(started_cluster_iceberg_no_spark.instances.values())
    table_name = "test_cluster_partition_pruning_reads_" + storage_type + "_" + get_uuid_str()

    # Identity partition on `a`; one distinct value per partition -> one data file each.
    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(a Int32, b Float64)",
        format_version=2,
        partition_by="a",
    )
    instance.query(
        f"INSERT INTO {table_name} VALUES (1,1.0),(2,2.0),(3,3.0),(4,4.0),(5,5.0)",
        settings={"allow_insert_into_iceberg": 1},
    )

    tf_single = get_creation_expression(
        storage_type, table_name, started_cluster_iceberg_no_spark, table_function=True
    )
    tf_cluster = get_creation_expression(
        storage_type, table_name, started_cluster_iceberg_no_spark,
        table_function=True, run_on_cluster=True,
    )

    # Correctness: cluster result matches single-node, with and without pruning.
    expected = instance.query(f"SELECT * FROM {tf_single} WHERE a = 1 ORDER BY ALL").strip()
    for prune in (0, 1):
        got = instance.query(
            f"SELECT * FROM {tf_cluster} WHERE a = 1 ORDER BY ALL",
            settings={"use_iceberg_partition_pruning": prune},
        ).strip()
        assert got == expected

    def files_read(table_function, prune):
        query_id = str(uuid.uuid4())
        instance.query(
            f"SELECT * FROM {table_function} WHERE a = 1 ORDER BY ALL",
            query_id=query_id,
            settings={"use_iceberg_partition_pruning": prune},
        )
        # No ZooKeeper here, so flush and read `system.query_log` on each present
        # node (`cluster_simple` lists node3, which is not started); the cluster
        # query itself only distributes to reachable replicas.
        total = 0
        for node in nodes:
            node.query("SYSTEM FLUSH LOGS")
            value = node.query(
                "SELECT sum(ProfileEvents['EngineFileLikeReadFiles']) "
                "FROM system.query_log "
                f"WHERE type = 'QueryFinish' AND initial_query_id = '{query_id}' "
                "AND ProfileEvents['EngineFileLikeReadFiles'] > 0"
            ).strip()
            total += int(value) if value and value != "\\N" else 0
        return total

    # The fixture is deterministic: 5 identity partitions, `WHERE a = 1`. Pin the
    # exact files opened so a shared partial-pruning regression on both paths
    # cannot pass. Without pruning all 5 data files are read; with pruning exactly
    # 1 is read on both the cluster and single-node paths.
    cluster_no_pruning = files_read(tf_cluster, 0)
    cluster_pruning = files_read(tf_cluster, 1)
    single_pruning = files_read(tf_single, 1)

    assert cluster_no_pruning == 5
    assert cluster_pruning == 1
    assert single_pruning == 1
