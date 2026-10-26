import json

import pytest

from helpers.paimon_utils import (
    create_spark_paimon_table,
    get_creation_expression,
    get_uuid_str,
    spark_table,
    upload_paimon_table,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_unsupported_row_type(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_row_type_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(
        spark, table_name, "id INT, nested STRUCT<x: INT, y: STRING>"
    )
    spark.sql(
        f"INSERT INTO {spark_table(table_name)} VALUES (1, named_struct('x', 1, 'y', 'a'))"
    )
    upload_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    error = instance.query_and_get_error(f"DESC {table_expr}")
    assert "Unsupported Paimon type: ROW" in error


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_unsupported_scan_mode(started_cluster_paimon_with_spark, storage_type):
    """Only scan.mode latest / latest-full / default are supported."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_scan_mode_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(
        spark,
        table_name,
        "a INT, b STRING",
        tblproperties={"scan.mode": "compacted-full"},
    )
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (1, 'one')")
    upload_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    error = instance.query_and_get_error(f"SELECT count() FROM {table_expr}")
    assert "scan mode" in error


def test_supported_scan_modes(started_cluster_paimon_with_spark):
    """The accepted scan.mode values must read fine (s3 only: the check is
    reader logic over schema options, storage-agnostic)."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    for mode in ("latest", "latest-full"):
        table_name = f"test_scan_mode_ok_{mode.replace('-', '_')}_" + get_uuid_str()
        create_spark_paimon_table(
            spark, table_name, "a INT, b STRING", tblproperties={"scan.mode": mode}
        )
        spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (1, 'one')")
        upload_paimon_table(started_cluster, "s3", table_name)

        table_expr = get_creation_expression(
            "s3", table_name, started_cluster, table_function=True
        )
        assert instance.query(f"SELECT count() FROM {table_expr}") == "1\n"


def test_unsupported_schema_version(started_cluster_paimon_with_spark):
    """Only schema version 3 is supported. No writer produces another version,
    so the schema file is corrupted by hand to pin the error path (s3 only:
    the check is reader logic, storage-agnostic)."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_schema_version_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, "a INT, b STRING")
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (1, 'one')")
    upload_paimon_table(started_cluster, "s3", table_name)

    schema_path = (
        "/var/lib/clickhouse/user_files/paimon_data/default.db/"
        f"{table_name}/schema/schema-0"
    )
    with open(schema_path) as f:
        schema = json.load(f)
    assert schema["version"] == 3
    schema["version"] = 4
    with open(schema_path, "w") as f:
        json.dump(schema, f)
    started_cluster.default_s3_uploader.upload_file(schema_path, schema_path)

    table_expr = get_creation_expression(
        "s3", table_name, started_cluster, table_function=True
    )
    error = instance.query_and_get_error(f"SELECT count() FROM {table_expr}")
    assert "is unsupported" in error


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_table_without_snapshots(started_cluster_paimon_with_spark, storage_type):
    """A Paimon table with a schema but no committed snapshot must produce a
    clean error or an empty result — never a hang or a server-side abort."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_no_snapshots_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, "a INT, b STRING")
    upload_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    stdout, stderr = instance.query_and_get_answer_with_error(
        f"SELECT count() FROM {table_expr}"
    )
    # Either an empty result or a clean DB::Exception is acceptable.
    assert stdout == "0\n" or "DB::Exception" in stderr
    assert instance.query("SELECT 1") == "1\n"


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_nonexistent_table_path(started_cluster_paimon_with_spark, storage_type):
    """A path with no Paimon table must produce a clean error, not a hang or a
    server-side abort."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_no_such_table_" + storage_type + "_" + get_uuid_str()

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    error = instance.query_and_get_error(f"SELECT count() FROM {table_expr}")
    assert error != ""
    # The server must stay healthy afterwards.
    assert instance.query("SELECT 1") == "1\n"
