import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
    default_upload_directory,
)


@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
@pytest.mark.parametrize("partition_type", ["", "identity(x)", "icebergBucket(3, x)"])
def test_writes_mutate_delete(started_cluster_iceberg_with_spark, storage_type, partition_type):
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_mutate_delete_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x String)", format_version, partition_type)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''
    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = 'pudge1000-7';", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (456);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n456\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (789), (890), (999);", settings={"allow_insert_into_iceberg": 1})

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = 'pudge1000-7';", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n456\n789\n890\n999\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = '789';", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n456\n890\n999\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = '123';", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '456\n890\n999\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = '999';", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '456\n890\n'

    if storage_type == "azure":
        return
    initial_files = default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query("SYSTEM ENABLE FAILPOINT iceberg_writes_cleanup")
    with pytest.raises(Exception):
        instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = '456';", settings={"allow_insert_into_iceberg": 1})

    files = default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    assert len(initial_files) == len(files)

    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 2

@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
def test_writes_mutate_delete_bug(started_cluster_iceberg_with_spark, storage_type):
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_mutate_delete_bug_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int)", format_version)
    instance.query(f"INSERT INTO TABLE {TABLE_NAME} VALUES (1)", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '1\n'
    instance.query(f"DELETE FROM {TABLE_NAME} WHERE TRUE", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''
    instance.query(f"INSERT INTO TABLE {TABLE_NAME} VALUES (2)", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '2\n'
    instance.query(f"DELETE FROM {TABLE_NAME} WHERE TRUE", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

@pytest.mark.parametrize("storage_type", ["s3"])
def test_writes_mutate_delete_forbid_first_version(started_cluster_iceberg_with_spark, storage_type):
    format_version = 1
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_mutate_delete_bug_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int)", format_version)
    instance.query(f"INSERT INTO TABLE {TABLE_NAME} VALUES (1)", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '1\n'

    with pytest.raises(Exception):
        instance.query(f"DELETE FROM {TABLE_NAME} WHERE TRUE", settings={"allow_insert_into_iceberg": 1})


def test_writes_mutate_after_partition_evolution(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    def create_table(remove_old_spec_files):
        table_name = "test_writes_mutate_after_partition_evolution_" + get_uuid_str()
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id long, region string, v string) USING iceberg PARTITIONED BY (region)
            TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'copy-on-write')
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'a', 'x'), (2, 'a', 'x')")
        spark.sql(f"ALTER TABLE {table_name} ADD PARTITION FIELD bucket(2, id)")
        spark.sql(f"INSERT INTO {table_name} VALUES (3, 'a', 'x'), (4, 'b', 'x')")
        if remove_old_spec_files:
            # Copy-on-write removes every spec-0 data file but keeps their manifest with only DELETED entries.
            spark.sql(f"DELETE FROM {table_name} WHERE id <= 2")
            manifests = spark.sql(
                f"SELECT partition_spec_id, added_data_files_count + existing_data_files_count "
                f"FROM spark_catalog.default.{table_name}.manifests WHERE content = 0"
            ).collect()
            assert [live for spec_id, live in manifests if spec_id == 0] == [0], manifests

        default_upload_directory(
            started_cluster_iceberg_with_spark,
            "local",
            f"/iceberg_data/default/{table_name}/",
            f"/iceberg_data/default/{table_name}/",
        )
        create_iceberg_table("local", instance, table_name, started_cluster_iceberg_with_spark, use_version_hint=True)
        return table_name

    table_name = create_table(remove_old_spec_files=False)
    for mutation in ["DELETE WHERE id = 3", "UPDATE v = 'y' WHERE id = 4"]:
        error = instance.query_and_get_error(f"ALTER TABLE {table_name} {mutation}", settings={"allow_insert_into_iceberg": 1})
        assert "ICEBERG_SPECIFICATION_VIOLATION" in error, error
    assert instance.query(f"SELECT id, v FROM {table_name} ORDER BY id") == "1\tx\n2\tx\n3\tx\n4\tx\n"

    table_name = create_table(remove_old_spec_files=True)
    instance.query(f"ALTER TABLE {table_name} DELETE WHERE id = 3", settings={"allow_insert_into_iceberg": 1})
    instance.query(f"ALTER TABLE {table_name} UPDATE v = 'y' WHERE id = 4", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT id, region, v FROM {table_name} ORDER BY id") == "4\tb\ty\n"

    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    default_download_directory(started_cluster_iceberg_with_spark, "local", f"{table_path}/", f"{table_path}/")
    assert [tuple(r) for r in spark.read.format("iceberg").load(table_path).collect()] == [(4, "b", "y")]
