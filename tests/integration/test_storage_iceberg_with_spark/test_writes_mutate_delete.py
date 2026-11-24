import glob
import json

import avro.datafile
import avro.io
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


def test_writes_mutate_rows_of_older_partition_spec(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_mutate_rows_of_older_partition_spec_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id long, v string) USING iceberg TBLPROPERTIES ('format-version' = '2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a'), (2, 'a')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (3, 'a'), (4, 'a')")
    spark.sql(f"ALTER TABLE {TABLE_NAME} ADD PARTITION FIELD bucket(2, id)")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a')")
    files_by_spec = dict(spark.sql(f"SELECT spec_id, count(*) FROM spark_catalog.default.{TABLE_NAME}.files GROUP BY spec_id").collect())
    assert files_by_spec[0] >= 2 and files_by_spec[1] >= 1, files_by_spec

    default_upload_directory(
        started_cluster_iceberg_with_spark, "local", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/"
    )
    create_iceberg_table("local", instance, TABLE_NAME, started_cluster_iceberg_with_spark, use_version_hint=True)

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE id IN (1, 3, 5, 6)", settings={"allow_insert_into_iceberg": 1})
    instance.query(f"ALTER TABLE {TABLE_NAME} UPDATE v = 'b' WHERE id IN (2, 7)", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT id, v FROM {TABLE_NAME} ORDER BY id") == "2\tb\n4\ta\n7\tb\n8\ta\n"

    default_download_directory(started_cluster_iceberg_with_spark, "local", f"{table_path}/", f"{table_path}/")
    rows = sorted(tuple(row) for row in spark.read.format("iceberg").load(table_path).collect())
    assert rows == [(2, "b"), (4, "a"), (7, "b"), (8, "a")]

    # The partition struct of a delete manifest is named after the fields of its own spec.
    delete_manifests = 0
    for manifest in glob.glob(f"{table_path}/metadata/*.avro"):
        with open(manifest, "rb") as f:
            reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            spec_names = [field["name"] for field in json.loads(reader.meta["partition-spec"])] if "partition-spec" in reader.meta else None
            entries = list(reader)
        for entry in entries:
            data_file = entry.get("data_file")
            if isinstance(data_file, dict) and data_file["content"] == 1:
                delete_manifests += 1
                assert list(data_file["partition"]) == spec_names, (manifest, data_file["partition"], spec_names)
    assert delete_manifests > 0


def test_writes_mutate_rows_of_removed_schema(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_mutate_rows_of_removed_schema_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    spark.sql(f"CREATE TABLE {TABLE_NAME} (id long, v string) USING iceberg TBLPROPERTIES ('format-version' = '2')")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a'), (2, 'a')")
    spark.sql(f"ALTER TABLE {TABLE_NAME} ADD COLUMN w int")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (3, 'b', 3)")
    spark.sql(
        f"CALL system.expire_snapshots(table => '{TABLE_NAME}', older_than => TIMESTAMP '2100-01-01 00:00:00', "
        "retain_last => 1, clean_expired_metadata => true)"
    )
    # The data file written under schema 0 is still live, but only its manifest header keeps schema 0.
    with open(f"{table_path}/metadata/version-hint.text") as f:
        metadata = json.load(open(f"{table_path}/metadata/v{f.read().strip()}.metadata.json"))
    assert [schema["schema-id"] for schema in metadata["schemas"]] == [1], metadata["schemas"]
    assert spark.sql(f"SELECT count(*) FROM spark_catalog.default.{TABLE_NAME}.files").collect()[0][0] == 2

    default_upload_directory(
        started_cluster_iceberg_with_spark, "local", f"/iceberg_data/default/{TABLE_NAME}/", f"/iceberg_data/default/{TABLE_NAME}/"
    )
    create_iceberg_table("local", instance, TABLE_NAME, started_cluster_iceberg_with_spark, use_version_hint=True)

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE id = 1", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT id, v FROM {TABLE_NAME} ORDER BY id") == "2\ta\n3\tb\n"

    default_download_directory(started_cluster_iceberg_with_spark, "local", f"{table_path}/", f"{table_path}/")
    rows = sorted(tuple(row) for row in spark.read.format("iceberg").load(table_path).collect())
    assert rows == [(2, "a", None), (3, "b", 3)]
