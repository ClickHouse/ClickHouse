import uuid
import pytest

from helpers.iceberg_utils import (
    generate_data,
    get_uuid_str,
    get_creation_expression,
    get_creation_expression,
    get_uuid_str,
    write_iceberg_from_df,
    default_upload_directory
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_metadata_cache(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_metadata_cache_" + storage_type + "_" + get_uuid_str()

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 10),
        TABLE_NAME,
        mode="overwrite",
        format_version="1",
        partition_by="a",
    )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    table_expr = get_creation_expression(storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True)

    query_id = f"{TABLE_NAME}-{uuid.uuid4()}"
    instance.query(
        f"SELECT * FROM {table_expr}", query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    assert 0 < int(
        instance.query(
            f"SELECT ProfileEvents['IcebergMetadataFilesCacheMisses'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    query_id = f"{TABLE_NAME}-{uuid.uuid4()}"
    instance.query(
        f"SELECT * FROM {table_expr}",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    assert 0 == int(
        instance.query(
            f"SELECT ProfileEvents['IcebergMetadataFilesCacheMisses'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    assert 0 < int(
        instance.query(
            f"SELECT ProfileEvents['IcebergMetadataFilesCacheHits'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    instance.query("SYSTEM DROP ICEBERG METADATA CACHE")

    query_id = f"{TABLE_NAME}-{uuid.uuid4()}"
    instance.query(
        f"SELECT * FROM {table_expr}", query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    assert 0 < int(
        instance.query(
            f"SELECT ProfileEvents['IcebergMetadataFilesCacheMisses'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    query_id = f"{TABLE_NAME}-{uuid.uuid4()}"
    instance.query(
        f"SELECT * FROM {table_expr}",
        query_id=query_id,
        settings={"use_iceberg_metadata_files_cache":"0"},
    )

    instance.query("SYSTEM FLUSH LOGS")
    assert "0\t0\n" == instance.query(
            f"SELECT ProfileEvents['IcebergMetadataFilesCacheHits'], ProfileEvents['IcebergMetadataFilesCacheMisses'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'",
        )