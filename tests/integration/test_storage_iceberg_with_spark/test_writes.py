import glob
import json
import os
import pytest
import pyarrow.parquet as pq

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    get_uuid_str,
    default_download_directory
)

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = "test_writes_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id int) USING iceberg TBLPROPERTIES ('format-version' = '{format_version}')")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (42);")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '42\n123\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (456);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '42\n123\n456\n'

    if storage_type == "azure":
        return

    initial_files = default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "wb") as f:
        f.write(b"4")

    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 3

    instance.query("SYSTEM ENABLE FAILPOINT iceberg_writes_cleanup")
    with pytest.raises(Exception):
        instance.query(f"INSERT INTO {TABLE_NAME} VALUES (777777777777);", settings={"allow_insert_into_iceberg": 1})


    files = default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    assert len(initial_files) == len(files)

@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_parquet_field_ids(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_field_ids_" + storage_type + "_" + get_uuid_str()
    local_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int32, label String, score Float64)",
        2,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'alice', 1.5), (2, 'bob', 2.5), (3, 'charlie', 3.5)",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert (
        instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id")
        == "1\talice\t1.5\n2\tbob\t2.5\n3\tcharlie\t3.5\n"
    )

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    metadata_dir = os.path.join(local_dir, "metadata")
    metadata_files = sorted(
        glob.glob(os.path.join(metadata_dir, "*.metadata.json")),
        key=os.path.getmtime,
    )
    assert metadata_files

    with open(metadata_files[-1]) as f:
        metadata = json.load(f)

    current_schema_id = metadata["current-schema-id"]
    current_schema = next(
        s for s in metadata["schemas"] if s["schema-id"] == current_schema_id
    )
    iceberg_field_ids = {field["name"]: field["id"] for field in current_schema["fields"]}
    expected_field_ids = {"id": 1, "label": 2, "score": 3}
    assert iceberg_field_ids == expected_field_ids

    data_dir = os.path.join(local_dir, "data")
    parquet_files = [
        f
        for f in glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True)
        if "delete" not in os.path.basename(f)
    ]
    assert parquet_files

    for path in parquet_files:
        schema = pq.read_schema(path)
        for field in schema:
            raw = field.metadata.get(b"PARQUET:field_id") if field.metadata else None
            assert raw is not None, (
                f"Column '{field.name}' in {os.path.basename(path)} has no field_id "
                f"in Parquet metadata."
            )
            actual_id = int(raw)
            expected_id = iceberg_field_ids[field.name]
            assert actual_id == expected_id, (
                f"Column '{field.name}': Parquet field_id={actual_id} does not match "
                f"Iceberg schema field_id={expected_id}."
            )


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_parquet_field_ids_complex_types(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_field_ids_complex_" + storage_type + "_" + get_uuid_str()
    local_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    schema = "(x Array(Nullable(Int32)), z Map(Int32, Nullable(Int64)), y Tuple(zip Nullable(Int32), foo Nullable(Int32)))"
    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        schema,
        2,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES ([1,2], {{5:6}}, (3,4))",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == "[1,2]\t{5:6}\t(3,4)\n"

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    metadata_dir = os.path.join(local_dir, "metadata")
    metadata_files = sorted(
        glob.glob(os.path.join(metadata_dir, "*.metadata.json")),
        key=os.path.getmtime,
    )
    assert metadata_files

    with open(metadata_files[-1]) as f:
        metadata = json.load(f)

    current_schema_id = metadata["current-schema-id"]
    current_schema = next(
        s for s in metadata["schemas"] if s["schema-id"] == current_schema_id
    )
    iceberg_field_ids = {field["name"]: field["id"] for field in current_schema["fields"]}
    expected_field_ids = {"x": 1, "z": 2, "y": 3}
    assert iceberg_field_ids == expected_field_ids

    data_dir = os.path.join(local_dir, "data")
    parquet_files = [
        f
        for f in glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True)
        if "delete" not in os.path.basename(f)
    ]
    assert parquet_files

    for path in parquet_files:
        schema_pq = pq.read_schema(path)
        for field in schema_pq:
            raw = field.metadata.get(b"PARQUET:field_id") if field.metadata else None
            assert raw is not None
            actual_id = int(raw)
            expected_id = iceberg_field_ids[field.name]
            assert actual_id == expected_id


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_parquet_field_ids_update(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_field_ids_update_" + storage_type + "_" + get_uuid_str()
    local_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(x String, y Int32)",
        2,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES ('alice', 1), ('bob', 2), ('charlie', 3)",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert (
        instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL")
        == "alice\t1\nbob\t2\ncharlie\t3\n"
    )

    instance.query(
        f"ALTER TABLE {TABLE_NAME} UPDATE x = 'dave' WHERE x = 'bob'",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert (
        instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL")
        == "alice\t1\ncharlie\t3\ndave\t2\n"
    )

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    data_dir = os.path.join(local_dir, "data")
    all_parquet = glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True)
    data_files = [f for f in all_parquet if "delete" not in os.path.basename(f)]
    delete_files = [f for f in all_parquet if "delete" in os.path.basename(f)]

    assert data_files
    assert delete_files

    expected_data_field_ids = {"x": 1, "y": 2}
    for path in data_files:
        schema_pq = pq.read_schema(path)
        for field in schema_pq:
            raw = field.metadata.get(b"PARQUET:field_id") if field.metadata else None
            assert raw is not None
            actual_id = int(raw)
            expected_id = expected_data_field_ids[field.name]
            assert actual_id == expected_id

    expected_delete_field_ids = {"file_path": 2147483546, "pos": 2147483545}
    for path in delete_files:
        schema_pq = pq.read_schema(path)
        for field in schema_pq:
            raw = field.metadata.get(b"PARQUET:field_id") if field.metadata else None
            assert raw is not None
            actual_id = int(raw)
            expected_id = expected_delete_field_ids[field.name]
            assert actual_id == expected_id


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
@pytest.mark.parametrize("format", ["ORC", "Avro"])
def test_writes_orc_format(started_cluster_iceberg_with_spark, format_version, storage_type, format):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_complex_types_" + storage_type + "_" + get_uuid_str()

    schema = "(x String)"
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, schema, format_version, format=format)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('Pavel Ivanov');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == 'Pavel Ivanov\n'

    if storage_type == "azure" or format != "ORC":
        return

    files = default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    found_orc_files = False
    for file in files:
        if file[-3:] == 'orc':
            found_orc_files = True
    assert found_orc_files

