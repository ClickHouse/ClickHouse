import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_uuid_str,
)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_low_cardinality(
    started_cluster_iceberg_with_spark, format_version, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_low_cardinality_" + storage_type + "_" + get_uuid_str()

    schema = (
        "(i Int32, "
        "s LowCardinality(String), "
        "n LowCardinality(Nullable(String)), "
        "a Array(LowCardinality(String)))"
    )
    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        schema,
        format_version,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'a', 'x', ['p', 'q']), (2, 'b', NULL, [])",
        settings={"allow_experimental_insert_into_iceberg": 1},
    )

    assert (
        instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL")
        == "1\ta\tx\t['p','q']\n2\tb\t\\N\t[]\n"
    )

    instance.query(f"DETACH TABLE {TABLE_NAME}")
    instance.query(f"ATTACH TABLE {TABLE_NAME}")
    assert instance.query(
        f"SELECT toTypeName(s), toTypeName(n), toTypeName(a) FROM {TABLE_NAME} LIMIT 1"
    ) == "LowCardinality(String)\tLowCardinality(Nullable(String))\tArray(LowCardinality(String))\n"

    assert (
        instance.query(f"SELECT count() FROM {TABLE_NAME} WHERE s = 'a'").strip() == "1"
    )

    if storage_type == "azure":
        return

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text",
        "wb",
    ) as f:
        f.write(b"1")

    df = (
        spark.read.format("iceberg")
        .load(
            f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
        )
        .sort("i")
        .collect()
    )
    assert len(df) == 2
    assert [row["s"] for row in df] == ["a", "b"]
    assert [row["n"] for row in df] == ["x", None]
    assert [row["a"] for row in df] == [["p", "q"], []]
