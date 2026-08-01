import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)
from helpers.s3_tools import list_s3_objects


def table_directory(table_name):
    return f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"


def count_table_files(started_cluster, instance, storage_type, table_name):
    if storage_type == "s3":
        return len(
            list_s3_objects(
                started_cluster.minio_client,
                started_cluster.minio_bucket,
                table_directory(table_name).lstrip("/"),
            )
        )
    if storage_type == "local":
        return int(
            instance.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"find {table_directory(table_name)} -type f 2>/dev/null | wc -l",
                ]
            ).strip()
        )
    raise Exception(f"Unknown iceberg storage type: {storage_type}")


def create_filled_table(started_cluster, instance, storage_type, table_name):
    create_iceberg_table(
        storage_type, instance, table_name, started_cluster, "(x Int32)"
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1), (2), (3)")
    files = count_table_files(started_cluster, instance, storage_type, table_name)
    assert files > 0
    return files


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_without_setting_keeps_data(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_drop_keeps_data_" + storage_type + "_" + get_uuid_str()

    files = create_filled_table(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name
    )

    instance.query(f"DROP TABLE {table_name} SYNC")

    assert (
        count_table_files(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name
        )
        == files
    )


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_with_settings_clause_deletes_data(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_drop_settings_clause_" + storage_type + "_" + get_uuid_str()

    create_filled_table(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name
    )

    instance.query(
        f"DROP TABLE {table_name} SYNC SETTINGS iceberg_delete_data_on_drop = 1"
    )

    assert (
        count_table_files(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name
        )
        == 0
    )


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_with_setting_sent_by_client_deletes_data(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_drop_client_setting_" + storage_type + "_" + get_uuid_str()

    create_filled_table(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name
    )

    instance.query(
        f"DROP TABLE {table_name} SYNC",
        settings={"iceberg_delete_data_on_drop": 1},
    )

    assert (
        count_table_files(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name
        )
        == 0
    )


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_database_with_settings_clause_deletes_data(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    suffix = storage_type + "_" + get_uuid_str()
    database_name = "test_drop_database_" + suffix
    table_name = f"{database_name}.t_{suffix}"

    instance.query(f"CREATE DATABASE {database_name}")

    create_filled_table(
        started_cluster_iceberg_no_spark, instance, storage_type, table_name
    )

    instance.query(
        f"DROP DATABASE {database_name} SYNC SETTINGS iceberg_delete_data_on_drop = 1"
    )

    assert (
        count_table_files(
            started_cluster_iceberg_no_spark, instance, storage_type, table_name
        )
        == 0
    )
