import time

import pytest

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str
from helpers.test_tools import assert_eq_with_retry


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


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_drop_deletes_data_of_table_not_read_since_attach(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = f"test_delete_data_on_drop_after_attach_{storage_type}_{get_uuid_str()}"

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x String, y Int64)",
    )
    instance.query(f"INSERT INTO {table_name} VALUES ('123', 1)")
    assert _files_left(started_cluster_iceberg_no_spark, storage_type, table_name) > 0

    # A re-attached table loads its metadata lazily, so nothing has read it before the DROP.
    instance.query(f"DETACH TABLE {table_name}")
    instance.query(f"ATTACH TABLE {table_name}")
    instance.query(
        f"DROP TABLE {table_name} SYNC",
        settings={"data_lake_delete_data_on_drop": 1},
    )

    assert _files_left(started_cluster_iceberg_no_spark, storage_type, table_name) == 0


def test_drop_deletes_data_after_restart_with_pending_drop(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = f"test_delete_data_on_drop_after_restart_{get_uuid_str()}"

    create_iceberg_table("s3", instance, table_name, started_cluster_iceberg_no_spark, "(x String, y Int64)")
    instance.query(f"INSERT INTO {table_name} VALUES ('123', 1)")

    # A dropped table is not removed while a query uses it.
    query_id = f"read_{get_uuid_str()}"
    instance.get_query_request(
        f"SELECT count() FROM {table_name} WHERE x NOT IN (SELECT toString(sleepEachRow(1)) FROM numbers(600))",
        settings={"max_block_size": 1},
        query_id=query_id,
        ignore_error=True,
    )
    assert_eq_with_retry(
        instance,
        f"SELECT count() FROM system.processes WHERE query_id = '{query_id}' AND read_rows > 0",
        "1",
    )
    instance.query(f"DROP TABLE {table_name}", settings={"data_lake_delete_data_on_drop": 1})
    assert _files_left(started_cluster_iceberg_no_spark, "s3", table_name) > 0

    # After a restart, the removal delay counts from the modification time of the dropped metadata.
    instance.stop_clickhouse(kill=True)
    instance.exec_in_container(
        ["bash", "-c", f"touch -d '1 day ago' /var/lib/clickhouse/metadata_dropped/default.{table_name}.*.sql"]
    )
    instance.start_clickhouse()

    for _ in range(120):
        if _files_left(started_cluster_iceberg_no_spark, "s3", table_name) == 0:
            break
        time.sleep(0.5)
    assert _files_left(started_cluster_iceberg_no_spark, "s3", table_name) == 0
