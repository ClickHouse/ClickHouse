import pytest

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str


def _table_dir(table_name):
    return f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"


def _files_left(instance, table_name):
    return int(
        instance.exec_in_container(
            ["bash", "-c", f"find {_table_dir(table_name)} -type f 2>/dev/null | wc -l"]
        ).strip()
    )


@pytest.mark.parametrize("delete_data_on_drop", [0, 1])
@pytest.mark.parametrize("storage_type", ["local"])
def test_drop_honours_query_level_delete_data_on_drop(
    started_cluster_iceberg_no_spark, storage_type, delete_data_on_drop
):
    # `StorageObjectStorage::drop` runs in a background thread, where the query context is already
    # gone. A query-level `data_lake_delete_data_on_drop` still has to reach it, because it is
    # captured by `IStorage::prepareForDrop` while the `DROP TABLE` query is running. Without that
    # capture the setting would only ever work when set server-wide in the default profile.
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = f"test_delete_data_on_drop_{delete_data_on_drop}_{get_uuid_str()}"

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x String, y Int64)",
    )
    instance.query(f"INSERT INTO {table_name} VALUES ('123', 1)")
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY ALL") == "123\t1\n"
    assert _files_left(instance, table_name) > 0

    instance.query(
        f"DROP TABLE {table_name} SYNC",
        settings={"data_lake_delete_data_on_drop": delete_data_on_drop},
    )

    if delete_data_on_drop:
        assert _files_left(instance, table_name) == 0
    else:
        assert _files_left(instance, table_name) > 0
