import pytest

from helpers.iceberg_utils import (
    started_cluster,
    default_upload_directory,
    get_uuid_str,
)

# JSON payloads with mixed types and one level of nesting, to exercise the variant reader.
ROWS = """
    (0, '{"a": 0, "b": [1, 2, 3], "s": "x"}'),
    (1, '{"a": 1, "b": [4, 5], "flag": true, "s": "y"}'),
    (2, '{"a": 2, "nested": {"k": true, "arr": [null, 1.5]}}')
"""


def test_clickhouse_reads_spark_variant(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table = "variant_" + get_uuid_str()

    # Spark 4.0's parse_json produces a native VARIANT column; write it as a plain Parquet file.
    spark.sql(
        f"SELECT id, parse_json(js) AS v FROM VALUES {ROWS} AS t(id, js) ORDER BY id"
    ).coalesce(1).write.mode("overwrite").parquet(
        f"/var/lib/clickhouse/user_files/{table}/"
    )
    default_upload_directory(started_cluster, "s3", f"/{table}/", f"/{table}/")

    url = f"http://minio1:9001/{started_cluster.minio_bucket}/var/lib/clickhouse/user_files/{table}/*.parquet"
    read = f"s3('{url}', 'minio', 'ClickHouse_Minio_P@ssw0rd', 'Parquet')"
    settings = {"enable_variant_type": 1}

    # Spark marks variant via its own key-value metadata rather than the standard VARIANT logical
    # type, so this exercises the reader's structural detection. It reads back as a ClickHouse
    # Variant; these top-level objects land in its Map(String, String) member.
    schema = instance.query(f"DESCRIBE TABLE {read}", settings=settings)
    assert "\tVariant(" in schema, schema

    result = instance.query(
        "SELECT id, variantType(v), v.`Map(String, String)`['a'], v.`Map(String, String)`['s'] "
        f"FROM {read} ORDER BY id",
        settings=settings,
    )
    assert result.splitlines() == [
        "0\tMap(String, String)\t0\tx",
        "1\tMap(String, String)\t1\ty",
        "2\tMap(String, String)\t2\t",
    ], result
