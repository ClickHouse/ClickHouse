import pytest
from datetime import datetime, timezone
import time

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    default_download_directory,
    get_uuid_str,
    get_last_snapshot
)

@pytest.mark.parametrize("storage_type", ["local", "s3", "azure"])
def test_optimize(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_" + storage_type + "_" + get_uuid_str()


    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(id Int32, data String)")
    snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    snapshot_timestamp = datetime.now(timezone.utc)

    time.sleep(0.1)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 110)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    instance.query(f"OPTIMIZE TABLE {TABLE_NAME};", settings={"allow_experimental_iceberg_compaction" : 1})

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == instance.query(
        "SELECT number FROM numbers(20, 90)"
    )

    # check that timetravel works with previous snapshot_ids and timestamps
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_timestamp_ms = {int(snapshot_timestamp.timestamp() * 1000)}") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )
    if storage_type != "local":
        return

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 90


@pytest.mark.parametrize("storage_type", ["local"])	
def test_optimize_background(started_cluster_iceberg_with_spark, storage_type):	
    instance = started_cluster_iceberg_with_spark.instances["node1"]	
    spark = started_cluster_iceberg_with_spark.spark_session	
    TABLE_NAME = "test_position_deletes_" + storage_type + "_" + get_uuid_str()	

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(id Int32, data String)")

    instance.query(f"""
    INSERT INTO {TABLE_NAME}
    SELECT
        number AS id,
        char(number + ascii('a')) AS ch
    FROM numbers(100)
    WHERE number >= 10;
    """, settings={"allow_experimental_insert_into_iceberg": 1})
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90	

    instance.query(f"DELETE FROM {TABLE_NAME} WHERE id < 20", settings={"allow_experimental_insert_into_iceberg": 1, "allow_experimental_iceberg_background_compaction":1})	

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80	
    initial_files = default_download_directory(	
        started_cluster_iceberg_with_spark,	
        storage_type,	
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",	
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",	
    )	

    time.sleep(10)	

    files = []	
    max_retries_count = 25	
    while True:	
        try:	
            files = default_download_directory(	
                started_cluster_iceberg_with_spark,	
                storage_type,	
                f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",	
                f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",	
            )	
            break	
        except:	
            # Catching exception means that compaction is in progress and we should wait a bit.	
            max_retries_count -= 1	
            if max_retries_count < 0:	
                raise ValueError("Compaction took too much time.")	
            time.sleep(2)	
            continue	

    assert len(initial_files) - len(files) == 5
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80	
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == instance.query(	
        "SELECT number FROM numbers(20, 80)"	
    )	
