import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_creation_expression,
    get_uuid_str,
)


PARALLEL_REPLICAS_SETTINGS = {
    "parallel_replicas_for_cluster_engines": 1,
    "enable_parallel_replicas": 2,
    "cluster_for_parallel_replicas": "cluster_simple",
}


# Writes to an Iceberg table that is not managed by a catalog, with parallel
# replicas enabled. `icebergS3Cluster` resolves to `StorageObjectStorageCluster`,
# which used to implement only the read side, so `INSERT` failed with
# `Method write is not supported by storage IcebergS3`.
@pytest.mark.parametrize("storage_type", ["s3"])
def test_writes_parallel_replicas_no_catalog(started_cluster_iceberg_no_spark, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = (
        "test_writes_parallel_replicas_no_catalog_" + storage_type + "_" + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
    )

    tf_single = get_creation_expression(
        storage_type, table_name, started_cluster_iceberg_no_spark, table_function=True
    )
    tf_cluster = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_no_spark,
        table_function=True,
        run_on_cluster=True,
    )

    # Engine table: stays a plain `StorageObjectStorage`.
    instance.query(
        f"INSERT INTO {table_name} VALUES (1)", settings=PARALLEL_REPLICAS_SETTINGS
    )
    # Plain table function: not auto-converted to the cluster variant for `INSERT`.
    instance.query(
        f"INSERT INTO FUNCTION {tf_single} VALUES (2)",
        settings=PARALLEL_REPLICAS_SETTINGS,
    )
    # Explicit cluster table function: the write is performed by the initiator.
    instance.query(
        f"INSERT INTO FUNCTION {tf_cluster} VALUES (3)",
        settings=PARALLEL_REPLICAS_SETTINGS,
    )

    expected = "1\n2\n3\n"
    for source in [table_name, f"{tf_single}", f"{tf_cluster}"]:
        assert (
            instance.query(
                f"SELECT x FROM {source} ORDER BY ALL",
                settings=PARALLEL_REPLICAS_SETTINGS,
            )
            == expected
        )
