import pytest

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str


def _table_path(table_name):
    return f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"


def _files_left(cluster, storage_type, table_name):
    if storage_type == "local":
        return int(
            cluster.instances["node1"]
            .exec_in_container(
                [
                    "bash",
                    "-c",
                    f"find /{_table_path(table_name)} -type f 2>/dev/null | wc -l",
                ]
            )
            .strip()
        )

    # The S3 table path is relative to the bucket root, unlike the absolute local one.
    return len(
        list(
            cluster.minio_client.list_objects(
                cluster.minio_bucket, f"{_table_path(table_name)}/", recursive=True
            )
        )
    )


@pytest.mark.parametrize("delete_data_on_drop", [0, 1])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_drop_honours_query_level_delete_data_on_drop(
    started_cluster_iceberg_no_spark, storage_type, delete_data_on_drop
):
    # `StorageObjectStorage::drop` runs in a background thread, where the query context is already gone.
    # A query-level `data_lake_delete_data_on_drop` reaches it only because `IStorage::prepareForDrop`
    # captures it while the `DROP TABLE` query is still running.
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = f"test_delete_data_on_drop_{storage_type}_{delete_data_on_drop}_{get_uuid_str()}"

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x String, y Int64)",
    )
    instance.query(f"INSERT INTO {table_name} VALUES ('123', 1)")
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY ALL") == "123\t1\n"
    assert _files_left(started_cluster_iceberg_no_spark, storage_type, table_name) > 0

    instance.query(
        f"DROP TABLE {table_name} SYNC",
        settings={"data_lake_delete_data_on_drop": delete_data_on_drop},
    )

    if delete_data_on_drop:
        assert (
            _files_left(started_cluster_iceberg_no_spark, storage_type, table_name) == 0
        )
    else:
        assert _files_left(started_cluster_iceberg_no_spark, storage_type, table_name) > 0
