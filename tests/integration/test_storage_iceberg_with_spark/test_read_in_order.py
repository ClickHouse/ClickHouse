import pytest
import json

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    create_iceberg_table
)


def get_array(query_result: str):
    arr = sorted([int(x) for x in query_result.strip().split("\n")])
    print(arr)
    return arr

def patch_metadata(table_name):
    # HACK This is terribly ugly hack, because of the issue:https://github.com/apache/iceberg/issues/13634
    # Iceberg sort order looks relatively new feature. There are no writer implementations which support it properly.
    # For example pyiceberg doesn't support it at all, you can specify sort order, but data will be written unsorted.
    # Spark implementation supports it, i.e. writes sorted data, but doesn't write proper sort_order_id in manifest files (always writes 0).
    # Here we manually modify metadata file to set actual sort order to id 0.
    with open(f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata/v4.metadata.json", "rb") as f:
        content = json.load(f)
        for order in content['sort-orders']:
            if order['order-id'] == 1:
                order_found = order
                break
        else:
            raise Exception("Sort order with id 1 not found")
        order_found['order-id'] = 0
        content['sort-orders'] = [order_found]
        content['default-sort-order-id'] = 0

        with open(f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata/v4.metadata.json", "w") as out_f:
            json.dump(content, out_f)
    # HACK END


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_read_in_order(started_cluster_iceberg_with_spark,  storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_read_in_order_" + storage_type + "_" + get_uuid_str()

    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (
            id BIGINT,
            data STRING
        )
        USING iceberg
    """)
    spark.sql(f"""
        ALTER TABLE {TABLE_NAME} 
        WRITE ORDERED BY id
    """)

    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1,'a'), (3, 'c')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2,'d'), (4, 'f')")

    patch_metadata(TABLE_NAME)

    files = default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    query_id = get_uuid_str()

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' not in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY id;"
        )
    )

    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY icebergBucket(16, id);"
        )
    )

    assert 'MergingSortedTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY id;"
        )
    )

    assert get_array(instance.query(f"SELECT distinct(id) FROM {TABLE_NAME}", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' not in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT distinct(id) FROM {TABLE_NAME};"
        )
    )

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY (id, data)", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY (id, data);"
        )
    )

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME}", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' not in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME};"
        )
    )

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY (data, id)", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY (data, id);"
        )
    )

@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_read_in_order_with_complex_bucket(started_cluster_iceberg_with_spark,  storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_position_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (
            id BIGINT,
            data STRING
        )
        USING iceberg
    """)
    spark.sql(f"""
        ALTER TABLE {TABLE_NAME} 
        WRITE ORDERED BY bucket(16, id)
    """)

    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1,'a'), (3, 'c')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2,'d'), (4, 'f')")

    patch_metadata(TABLE_NAME)

    files = default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    query_id = get_uuid_str()

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY icebergBucket(16, id)", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY id;"
        )
    )

    assert 'PartialSortingTransform' not in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY icebergBucket(16, id);"
        )
    )

@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_read_in_order_with_complex_truncate(started_cluster_iceberg_with_spark,  storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_position_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (
            id BIGINT,
            data STRING
        )
        USING iceberg
    """)
    spark.sql(f"""
        ALTER TABLE {TABLE_NAME} 
        WRITE ORDERED BY truncate(16, id)
    """)

    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1,'a'), (3, 'c')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2,'d'), (4, 'f')")

    patch_metadata(TABLE_NAME)

    files = default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    query_id = get_uuid_str()

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY icebergTruncate(16, id)", query_id=query_id)) == [1,2,3,4]
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY id;"
        )
    )

    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY icebergBucket(16, id);"
        )
    )

    assert 'PartialSortingTransform' not in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT * FROM {TABLE_NAME} ORDER BY icebergTruncate(16, id);"
        )
    )
