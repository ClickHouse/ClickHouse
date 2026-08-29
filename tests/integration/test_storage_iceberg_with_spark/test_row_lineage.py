import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    drop_iceberg_table,
    get_creation_expression,
    get_uuid_str,
)


def _spark_lineage(spark, table_name):
    rows = spark.sql(
        f"SELECT id, _row_id, _last_updated_sequence_number FROM {table_name}"
    ).collect()
    return {
        row["id"]: (row["_row_id"], row["_last_updated_sequence_number"]) for row in rows
    }


def _clickhouse_lineage(instance, table_expression, where="", settings=None):
    raw = instance.query(
        f"SELECT id, _row_id, _last_updated_sequence_number FROM {table_expression} {where} FORMAT TSV",
        settings=settings,
    )

    def parse(value):
        return None if value == "\\N" else int(value)

    lineage = {}
    for line in raw.strip().split("\n"):
        if not line:
            continue
        row_key, row_id, sequence_number = line.split("\t")
        lineage[int(row_key)] = (parse(row_id), parse(sequence_number))
    return lineage


def _row_ids(lineage):
    return {row_key: row_id for row_key, (row_id, _) in lineage.items()}


def _publish(started_cluster, storage_type, table_name):
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )


@pytest.mark.parametrize("run_on_cluster", [False, True])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_lineage_inherited_from_manifest(
    started_cluster_iceberg_with_spark, storage_type, run_on_cluster
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_lineage_inherited_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3')"
    )
    for lo in range(0, 40, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range({lo}, {lo + 10})"
        )

    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
        run_on_cluster=run_on_cluster,
    )

    assert int(instance.query(f"SELECT count() FROM {table_expression}")) == 40

    spark_lineage = _spark_lineage(spark, TABLE_NAME)

    assert sorted(row_id for row_id, _ in spark_lineage.values()) == list(range(40))
    for row_key, (row_id, sequence_number) in spark_lineage.items():
        assert row_id == row_key
        assert sequence_number == row_key // 10 + 1

    assert _clickhouse_lineage(instance, table_expression) == spark_lineage


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_id_inherited_for_several_files_in_one_manifest(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_id_one_manifest_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, part int) USING iceberg "
        f"PARTITIONED BY (part) TBLPROPERTIES ('format-version' = '3')"
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, id % 4 from range(0, 20)")

    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    assert int(instance.query(f"SELECT count() FROM {table_expression}")) == 20

    spark_lineage = _spark_lineage(spark, TABLE_NAME)

    assert sorted(row_id for row_id, _ in spark_lineage.values()) == list(range(20))
    assert all(sequence_number == 1 for _, sequence_number in spark_lineage.values())

    assert _clickhouse_lineage(instance, table_expression) == spark_lineage


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_id_is_not_affected_by_filter_pushdown(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_id_filter_pushdown_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'write.parquet.row-group-size-bytes' = '100')"
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, 'a' from range(0, 10)")
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, 'b' from range(10, 20)")

    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    assert _row_ids(
        _clickhouse_lineage(instance, table_expression, where="WHERE id >= 15")
    ) == {row_key: row_key for row_key in range(15, 20)}

    assert _row_ids(
        _clickhouse_lineage(instance, table_expression, where="WHERE id % 7 = 3")
    ) == {3: 3, 10: 10, 17: 17}


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_lineage_materialized_after_update(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_lineage_materialized_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'write.update.mode' = 'copy-on-write')"
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, 'a' from range(0, 4)")
    spark.sql(f"UPDATE {TABLE_NAME} SET data = 'z' WHERE id = 1")

    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    assert int(instance.query(f"SELECT count() FROM {table_expression}")) == 4

    spark_lineage = _spark_lineage(spark, TABLE_NAME)

    assert spark_lineage == {0: (0, 1), 1: (1, 2), 2: (2, 1), 3: (3, 1)}

    assert _clickhouse_lineage(instance, table_expression) == spark_lineage


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_id_survives_copy_on_write_delete(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_id_after_delete_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'copy-on-write')"
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, 'a' from range(0, 4)")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 1")

    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    assert int(instance.query(f"SELECT count() FROM {table_expression}")) == 3

    spark_lineage = _spark_lineage(spark, TABLE_NAME)

    assert _row_ids(spark_lineage) == {0: 0, 2: 2, 3: 3}

    assert _clickhouse_lineage(instance, table_expression) == spark_lineage


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_lineage_is_null_for_v2_table(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_lineage_v2_null_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '2')"
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, 'a' from range(0, 4)")

    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    table_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    assert _clickhouse_lineage(instance, table_expression) == {
        row_key: (None, None) for row_key in range(4)
    }


@pytest.mark.parametrize("storage_type", ["s3"])
def test_first_row_id_in_system_iceberg_files(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_first_row_id_system_table_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '3')"
    )
    for lo in range(0, 40, 10):
        spark.sql(f"INSERT INTO {TABLE_NAME} select id, 'a' from range({lo}, {lo + 10})")

    _publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        format_version=3,
    )

    # Spark writes the entries without an explicit first_row_id and it is assigned from the manifest
    # list at read time, so the system table is where the resolved value can be observed.
    assert instance.query(
        f"SELECT first_row_id FROM system.iceberg_files "
        f"WHERE database = currentDatabase() AND table = '{TABLE_NAME}' AND content = 'DATA' "
        f"ORDER BY first_row_id FORMAT TSV"
    ).split() == ["0", "10", "20", "30"]

    drop_iceberg_table(instance, TABLE_NAME)
