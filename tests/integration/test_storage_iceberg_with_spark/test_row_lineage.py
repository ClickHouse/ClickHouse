import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    default_download_directory,
    drop_iceberg_table,
    get_creation_expression,
    get_uuid_str,
)


def _spark_lineage(spark, table_name):
    """Read the lineage of a table by path, which works for a table Spark never created itself.
    `_row_id` and `_last_updated_sequence_number` are metadata columns: they are not part of the
    schema a plain `collect` returns, so they have to be selected by name."""
    rows = (
        spark.read.format("iceberg")
        .load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}")
        .select("id", "_row_id", "_last_updated_sequence_number")
        .collect()
    )
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


def _fetch(started_cluster, storage_type, table_name):
    """The reverse of `_publish`: bring a table written by ClickHouse to the path Spark reads. The
    download helper takes the storage path as it is, so it has to be spelled out in full."""
    path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"
    default_download_directory(started_cluster, storage_type, path, path)


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

# The tests above have Spark write the table and ClickHouse read it. The ones below are the mirror
# image: ClickHouse writes, and Spark is the reference for what the row lineage of the result means.
INSERT_SETTINGS = {"allow_insert_into_iceberg": 1}


def _create_clickhouse_table(started_cluster, storage_type, table_name, schema, format_version=3, partition_by=""):
    create_iceberg_table(
        storage_type,
        started_cluster.instances["node1"],
        table_name,
        started_cluster,
        schema,
        format_version=format_version,
        partition_by=partition_by,
    )


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_lineage_clickhouse(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_lineage_written_by_clickhouse_" + storage_type + "_" + get_uuid_str()

    _create_clickhouse_table(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, "(id Int32, s String)"
    )

    # One row per INSERT, so every row lands in its own snapshot and gets its own sequence number.
    for row_key in range(40):
        instance.query(
            f"INSERT INTO {TABLE_NAME} VALUES ({row_key}, 'a')", settings=INSERT_SETTINGS
        )

    _fetch(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)

    spark_lineage = _spark_lineage(spark, TABLE_NAME)

    assert sorted(row_id for row_id, _ in spark_lineage.values()) == list(range(40))
    for row_key, (row_id, sequence_number) in spark_lineage.items():
        assert row_id == row_key
        assert sequence_number == row_key + 1

    assert _clickhouse_lineage(instance, TABLE_NAME) == spark_lineage


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_id_clickhouse_several_files_in_one_manifest(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_row_id_clickhouse_one_manifest_" + storage_type + "_" + get_uuid_str()

    _create_clickhouse_table(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, "(id Int32, s String)"
    )

    # A single snapshot whose manifest lists four data files: the row ids of the second and later
    # files are only right if the reader accumulates the record counts of the entries before them.
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number, 'a' FROM numbers(20)",
        settings={**INSERT_SETTINGS, "iceberg_insert_max_rows_in_data_file": 5, "max_insert_threads": 1},
    )

    _fetch(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)

    spark_lineage = _spark_lineage(spark, TABLE_NAME)

    assert sorted(row_id for row_id, _ in spark_lineage.values()) == list(range(20))
    assert all(sequence_number == 1 for _, sequence_number in spark_lineage.values())

    assert _clickhouse_lineage(instance, TABLE_NAME) == spark_lineage


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_id_clickhouse_is_not_affected_by_filter_pushdown(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_row_id_clickhouse_pushdown_" + storage_type + "_" + get_uuid_str()

    _create_clickhouse_table(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, "(id Int32, s String)"
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number, 'a' FROM numbers(10)", settings=INSERT_SETTINGS
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number, 'b' FROM numbers(10, 10)", settings=INSERT_SETTINGS
    )

    # A row id is the position of the row in the table, not in whatever part of the file survived
    # the filter, so it must not move when rows or row groups are skipped.
    assert _row_ids(_clickhouse_lineage(instance, TABLE_NAME, where="WHERE id >= 15")) == {
        row_key: row_key for row_key in range(15, 20)
    }

    assert _row_ids(_clickhouse_lineage(instance, TABLE_NAME, where="WHERE id % 7 = 3")) == {
        3: 3,
        10: 10,
        17: 17,
    }


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_id_clickhouse_survives_delete(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_row_id_clickhouse_after_delete_" + storage_type + "_" + get_uuid_str()

    _create_clickhouse_table(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, "(id Int32, s String)"
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number, 'a' FROM numbers(4)", settings=INSERT_SETTINGS
    )
    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE id = 1", settings=INSERT_SETTINGS)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 3

    # Deleting a row does not renumber the rows that stay.
    assert _row_ids(_clickhouse_lineage(instance, TABLE_NAME)) == {0: 0, 2: 2, 3: 3}


@pytest.mark.parametrize("storage_type", ["s3"])
def test_row_lineage_clickhouse_is_null_for_v2_table(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_row_lineage_clickhouse_v2_null_" + storage_type + "_" + get_uuid_str()

    _create_clickhouse_table(
        started_cluster_iceberg_with_spark,
        storage_type,
        TABLE_NAME,
        "(id Int32, s String)",
        format_version=2,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number, 'a' FROM numbers(4)", settings=INSERT_SETTINGS
    )

    assert _clickhouse_lineage(instance, TABLE_NAME) == {
        row_key: (None, None) for row_key in range(4)
    }


@pytest.mark.parametrize("storage_type", ["s3"])
def test_first_row_id_in_system_iceberg_files_clickhouse(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_first_row_id_system_table_clickhouse_" + storage_type + "_" + get_uuid_str()

    _create_clickhouse_table(
        started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, "(id Int32, s String)"
    )

    for lo in range(0, 40, 10):
        instance.query(
            f"INSERT INTO {TABLE_NAME} SELECT number, 'a' FROM numbers({lo}, 10)",
            settings=INSERT_SETTINGS,
        )

    # ClickHouse writes the manifest entries without an explicit first_row_id as well, so the values
    # here are the ones resolved from the first_row_id of the manifest list entry.
    assert instance.query(
        f"SELECT first_row_id FROM system.iceberg_files "
        f"WHERE database = currentDatabase() AND table = '{TABLE_NAME}' AND content = 'DATA' "
        f"ORDER BY first_row_id FORMAT TSV"
    ).split() == ["0", "10", "20", "30"]

    drop_iceberg_table(instance, TABLE_NAME)
