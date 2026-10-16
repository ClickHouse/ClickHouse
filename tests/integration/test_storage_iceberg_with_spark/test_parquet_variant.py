from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
)

# JSON payloads with mixed types and one level of nesting, to exercise the variant reader.
ROWS = """
    (0, '{"a": 0, "b": [1, 2, 3], "s": "x"}'),
    (1, '{"a": 1, "b": [4, 5], "flag": true, "s": "y"}'),
    (2, '{"a": 2, "nested": {"k": true, "arr": [null, 1.5]}}')
"""


def test_clickhouse_reads_spark_variant(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table = "variant_" + get_uuid_str()

    # Spark 4.0's parse_json produces a native VARIANT column; write it as a plain Parquet file.
    spark.sql(
        f"SELECT id, parse_json(js) AS v FROM VALUES {ROWS} AS t(id, js) ORDER BY id"
    ).coalesce(1).write.mode("overwrite").parquet(
        f"/var/lib/clickhouse/user_files/{table}/"
    )
    default_upload_directory(started_cluster_iceberg_with_spark, "s3", f"/{table}/", f"/{table}/")

    url = f"http://minio1:9001/{started_cluster_iceberg_with_spark.minio_bucket}/var/lib/clickhouse/user_files/{table}/*.parquet"
    read = f"s3('{url}', 'minio', 'ClickHouse_Minio_P@ssw0rd', 'Parquet')"

    # Spark marks variant via its own key-value metadata rather than the standard VARIANT logical
    # type, so this exercises the reader's structural detection. It reads back as `Dynamic`, and
    # these top-level objects land in its `JSON` type.
    schema = instance.query(f"DESCRIBE TABLE {read}")
    assert "v\tDynamic" in schema, schema

    result = instance.query(f"SELECT id, dynamicType(v), v FROM {read} ORDER BY id")
    assert result.splitlines() == [
        '0\tJSON\t{"a":0,"b":[1,2,3],"s":"x"}',
        '1\tJSON\t{"a":1,"b":[4,5],"flag":true,"s":"y"}',
        '2\tJSON\t{"a":2,"nested":{"arr":[null,1.5],"k":true}}',
    ], result
