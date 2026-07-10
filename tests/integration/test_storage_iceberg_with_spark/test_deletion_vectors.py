import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    get_creation_expression,
)


def get_array(query_result: str):
    return sorted([int(x) for x in query_result.strip().split("\n") if x])


@pytest.mark.parametrize("run_on_cluster", [False, True])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_deletion_vectors(started_cluster_iceberg_with_spark, storage_type, run_on_cluster):
    if storage_type == "local" and run_on_cluster:
        pytest.skip("Local storage with cluster execution is not supported")

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_deletion_vectors_" + storage_type + "_" + get_uuid_str()
    deleted_ids = [2, 5, 7, 100]

    spark.sql(
        f"""
        CREATE TABLE {table_name} (id bigint) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(f"INSERT INTO {table_name} SELECT id FROM range(0, 200)")
    spark.sql(
        f"DELETE FROM {table_name} WHERE id IN ({', '.join(str(x) for x in deleted_ids)})"
    )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )

    expression = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        run_on_cluster=run_on_cluster,
        table_function=True,
    )

    assert int(instance.query(f"SELECT count() FROM {expression}")) == 200 - len(deleted_ids)
    assert get_array(instance.query(f"SELECT id FROM {expression}")) == [
        x for x in range(200) if x not in deleted_ids
    ]
