import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
@pytest.mark.parametrize("other_format", ["orc", "avro"])
@pytest.mark.parametrize("use_prewhere", [False, True])
def test_mixed_file_formats_subcolumns(started_cluster_iceberg_with_spark, storage_type, other_format, use_prewhere):
    """
    A `Parquet`-configured Iceberg table whose data files are partly in another format. Subcolumns
    of a struct (`s.a`) are planned as direct reads from the table-level format, which the `Parquet`
    reader supports; a file in a format whose reader does not must read the whole struct and have
    the subcolumn extracted afterwards instead of treating `s.a` as a missing column.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_mixed_file_formats_subcolumns_" + storage_type + "_" + other_format + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id INT, s struct<a : INT, b : STRING>)
        USING iceberg
        TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, named_struct('a', 10, 'b', 'parquet'))")
    spark.sql(f"ALTER TABLE {TABLE_NAME} SET TBLPROPERTIES ('write.format.default' = '{other_format}')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (2, named_struct('a', 20, 'b', 'other'))")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query(
        get_creation_expression(storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=False)
    )

    settings = {"input_format_parquet_use_native_reader_v3": 1}
    where = "PREWHERE" if use_prewhere else "WHERE"

    assert instance.query(f"SELECT id, s.a, s.b FROM {TABLE_NAME} ORDER BY id", settings=settings) == "1\t10\tparquet\n2\t20\tother\n"
    assert instance.query(f"SELECT s.a FROM {TABLE_NAME} ORDER BY s.a", settings=settings) == "10\n20\n"
    assert instance.query(f"SELECT id FROM {TABLE_NAME} {where} s.a = 20", settings=settings) == "2\n"
    assert instance.query(f"SELECT id, s FROM {TABLE_NAME} {where} s.b = 'other'", settings=settings) == "2\t(20,'other')\n"
    assert instance.query(f"SELECT count() FROM {TABLE_NAME} {where} s.a > 0", settings=settings) == "2\n"

    instance.query(f"DROP TABLE {TABLE_NAME}")
