import glob
import json
import os
import pytest
import pyarrow.parquet as pq

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
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

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_detach_attach(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_detach_attach_" + storage_type + "_" + get_uuid_str()

    schema = "(x String)"
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, schema, format_version)

    instance.query(f"DETACH TABLE {TABLE_NAME}")
    instance.query(f"ATTACH TABLE {TABLE_NAME}")

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('Pavel Ivanov');", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == 'Pavel Ivanov\n'


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_restart(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_restart_" + storage_type + "_" + get_uuid_str()

    schema = "(x String)"
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, schema, format_version)

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('before restart');", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == 'before restart\n'

    instance.restart_clickhouse()

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == 'before restart\n'

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('after restart');", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == 'after restart\nbefore restart\n'



DECIMAL_COLUMNS = [
    ("small", "Decimal(7, 2)", "decimal(7, 2)", ["17.22", "-8888.99", "0.01", "0.00"]),
    ("medium", "Decimal(18, 4)", "decimal(18, 4)", ["99999999999999.9999", "-99999999999999.9999", "1.0000", "0.0000"]),
    ("large", "Decimal(38, 10)", "decimal(38, 10)", ["9999999999999999999999999999.9999999999", "-1.0000000001", "0.0000000000", "2.5000000000"]),
]


def read_latest_metadata(local_dir):
    metadata_files = sorted(
        glob.glob(os.path.join(local_dir, "metadata", "*.metadata.json")),
        key=os.path.getmtime,
    )
    assert metadata_files
    with open(metadata_files[-1]) as f:
        return json.load(f)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_decimal(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_decimal_" + storage_type + "_" + format_version + "_" + get_uuid_str()
    local_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    schema = "(" + ", ".join(f"{name} {ch_type}" for name, ch_type, _, _ in DECIMAL_COLUMNS) + ")"
    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, schema, format_version
    )

    rows = [
        tuple(values[i] for _, _, _, values in DECIMAL_COLUMNS)
        for i in range(len(DECIMAL_COLUMNS[0][3]))
    ]
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES {', '.join('(' + ', '.join(row) + ')' for row in rows)}",
        settings={"allow_insert_into_iceberg": 1},
    )

    expected = "".join("\t".join(row) + "\n" for row in sorted(rows, key=lambda row: [float(value) for value in row]))
    assert (
        instance.query(
            f"SELECT * FROM {TABLE_NAME} ORDER BY ALL",
            settings={"output_format_decimal_trailing_zeros": 1},
        )
        == expected
    )

    type_names = ", ".join(f"toTypeName({name})" for name, _, _, _ in DECIMAL_COLUMNS)
    assert instance.query(f"SELECT {type_names} FROM {TABLE_NAME} LIMIT 1") == "\t".join(
        ch_type for _, ch_type, _, _ in DECIMAL_COLUMNS
    ) + "\n"

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"{local_dir}/",
        f"{local_dir}/",
    )

    metadata = read_latest_metadata(local_dir)
    current_schema = next(
        s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"]
    )
    written_types = {field["name"]: field["type"] for field in current_schema["fields"]}
    assert written_types == {name: iceberg_type for name, _, iceberg_type, _ in DECIMAL_COLUMNS}

    data_files = [
        f
        for f in glob.glob(os.path.join(local_dir, "data", "**", "*.parquet"), recursive=True)
        if "delete" not in os.path.basename(f)
    ]
    assert data_files
    for path in data_files:
        parquet_schema = pq.read_schema(path)
        for name, ch_type, _, _ in DECIMAL_COLUMNS:
            field_type = parquet_schema.field(name).type
            precision, scale = ch_type[len("Decimal("):-1].split(", ")
            assert str(field_type) == f"decimal128({precision}, {scale})", (name, str(field_type))


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_decimal_read_by_spark(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_decimal_spark_" + storage_type + "_" + get_uuid_str()
    local_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int32, price Decimal(18, 4))",
        2,
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 17.2200), (2, -8888.9900), (3, 99999999999999.9999)",
        settings={"allow_insert_into_iceberg": 1},
    )

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"{local_dir}/",
        f"{local_dir}/",
    )

    metadata_versions = [
        int(os.path.basename(path).split(".")[0][1:])
        for path in glob.glob(os.path.join(local_dir, "metadata", "v*.metadata.json"))
    ]
    with open(os.path.join(local_dir, "metadata", "version-hint.text"), "wb") as f:
        f.write(str(max(metadata_versions)).encode())

    spark_rows = spark.read.format("iceberg").load(local_dir).orderBy("id").collect()
    assert [str(row["price"]) for row in spark_rows] == [
        "17.2200",
        "-8888.9900",
        "99999999999999.9999",
    ]


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_decimal_minmax_pruning(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_decimal_minmax_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(small Decimal(7, 2), wide Decimal(18, 4), control Int64)",
        2,
    )

    # One file per insert, so the min/max bounds of every file cover exactly one row. `control`
    # carries the same magnitudes in a type whose bounds are known to prune, to tell a decimal-specific
    # gap apart from a table that simply cannot be pruned.
    for small, wide, control in [
        ("1.00", "1.0000", "1"),
        ("500.00", "99999999999999.9999", "500"),
        ("-8888.99", "-99999999999999.9999", "-8888"),
    ]:
        instance.query(
            f"INSERT INTO {TABLE_NAME} VALUES ({small}, {wide}, {control})",
            settings={"allow_insert_into_iceberg": 1},
        )

    base_settings = {
        "input_format_parquet_bloom_filter_push_down": 0,
        "input_format_parquet_filter_push_down": 0,
        "output_format_decimal_trailing_zeros": 1,
    }

    def measure(predicate, index):
        query = f"SELECT * FROM {TABLE_NAME} {predicate} ORDER BY ALL"
        without_pruning = instance.query(
            query, settings={**base_settings, "use_iceberg_partition_pruning": 0}
        )
        query_id = f"{TABLE_NAME}-{index}"
        with_pruning = instance.query(
            query,
            query_id=query_id,
            settings={**base_settings, "use_iceberg_partition_pruning": 1},
        )
        instance.query("SYSTEM FLUSH LOGS")
        pruned = instance.query(
            f"SELECT ProfileEvents['IcebergMinMaxIndexPrunedFiles'] FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        ).strip()
        return without_pruning, with_pruning, int(pruned or 0)

    predicates = [
        "",
        "WHERE control < -1000",
        "WHERE small < -1000",
        "WHERE small > 100",
        "WHERE wide > 1000000",
        "WHERE wide < -1000000",
    ]
    measured = {
        predicate: measure(predicate, index) for index, predicate in enumerate(predicates)
    }

    # Pruning must never change the result, whatever it manages to skip.
    for predicate, (without_pruning, with_pruning, _) in measured.items():
        assert without_pruning == with_pruning, predicate

    assert measured[""][2] == 0
    assert measured["WHERE control < -1000"][2] == 2
    assert measured["WHERE small < -1000"][2] == 2
    assert measured["WHERE small > 100"][2] == 2
    # The bounds of `wide` take all 8 bytes of the underlying Int64.
    assert measured["WHERE wide > 1000000"][2] == 2
    assert measured["WHERE wide < -1000000"][2] == 2


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_decimal_partition(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_decimal_partition_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(tag Decimal(7, 2), number Int64)",
        2,
        partition_by="tag",
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1.50, 10), (2.50, 20), (1.50, 30), (-3.25, 40)",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert (
        instance.query(
            f"SELECT * FROM {TABLE_NAME} ORDER BY ALL",
            settings={"output_format_decimal_trailing_zeros": 1},
        )
        == "-3.25\t40\n1.50\t10\n1.50\t30\n2.50\t20\n"
    )

    def prunned_files(select_expression):
        return check_validity_and_get_prunned_files_general(
            instance,
            TABLE_NAME,
            {"use_iceberg_partition_pruning": 0},
            {"use_iceberg_partition_pruning": 1},
            "IcebergPartitionPrunedFiles",
            select_expression,
        )

    assert prunned_files(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == 0
    assert prunned_files(f"SELECT * FROM {TABLE_NAME} WHERE tag < 0 ORDER BY ALL") == 2
    assert prunned_files(f"SELECT * FROM {TABLE_NAME} WHERE tag > 2 ORDER BY ALL") == 2


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_decimal_partition_not_first(started_cluster_iceberg_with_spark, storage_type):
    """Every partition field must be serialized against its own Avro node.

    The decimal is deliberately neither the first nor the last partition field, and it is
    surrounded by fields of other shapes: a plain `long`, a `Nullable` union whose branch has to
    be picked from the union's own schema, and a `DateTime64`, which shares `DecimalField` with a
    decimal but goes into the manifest as a `long`.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_decimal_partition_not_first_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int64, note Nullable(String), tag Decimal(7, 2), ts DateTime64(6), number Int64)",
        2,
        partition_by="(id, note, tag, ts)",
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES "
        f"(1, 'a', 1.50, '2025-08-27 12:34:56.000000', 10), "
        f"(1, NULL, -3.25, '2025-08-27 12:34:56.000000', 20), "
        f"(2, 'a', 1.50, '2026-08-27 12:34:56.000000', 30)",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert (
        instance.query(
            f"SELECT * FROM {TABLE_NAME} ORDER BY ALL",
            settings={"output_format_decimal_trailing_zeros": 1},
        )
        # `ORDER BY` puts NULLs last.
        == "1\ta\t1.50\t2025-08-27 12:34:56.000000\t10\n"
        "1\t\\N\t-3.25\t2025-08-27 12:34:56.000000\t20\n"
        "2\ta\t1.50\t2026-08-27 12:34:56.000000\t30\n"
    )

    def prunned_files(select_expression):
        return check_validity_and_get_prunned_files_general(
            instance,
            TABLE_NAME,
            {"use_iceberg_partition_pruning": 0},
            {"use_iceberg_partition_pruning": 1},
            "IcebergPartitionPrunedFiles",
            select_expression,
        )

    assert prunned_files(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == 0
    assert prunned_files(f"SELECT * FROM {TABLE_NAME} WHERE tag < 0 ORDER BY ALL") == 2
    assert prunned_files(f"SELECT * FROM {TABLE_NAME} WHERE tag > 1 ORDER BY ALL") == 1
    assert prunned_files(f"SELECT * FROM {TABLE_NAME} WHERE id = 2 ORDER BY ALL") == 2
    assert (
        prunned_files(f"SELECT * FROM {TABLE_NAME} WHERE ts > '2026-01-01 00:00:00' ORDER BY ALL")
        == 2
    )


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_decimal_partition_same_shape(started_cluster_iceberg_with_spark, storage_type):
    """Two partition columns of the same decimal shape must not collide on the Avro `fixed` name.

    `fixed` is a named Avro type whose generated name used to depend only on `(precision, scale)`,
    so a spec like `PARTITION BY (price_a, price_b)` with two `Decimal(18, 4)` columns emitted two
    definitions of the same name into one manifest schema and failed schema compilation on insert.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_decimal_partition_same_shape_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(price_a Decimal(18, 4), price_b Decimal(18, 4), number Int64)",
        2,
        partition_by="(price_a, price_b)",
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1.5000, 2.5000, 10), (-3.2500, 4.7500, 20)",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert (
        instance.query(
            f"SELECT * FROM {TABLE_NAME} ORDER BY ALL",
            settings={"output_format_decimal_trailing_zeros": 1},
        )
        == "-3.2500\t4.7500\t20\n1.5000\t2.5000\t10\n"
    )

    def prunned_files(select_expression):
        return check_validity_and_get_prunned_files_general(
            instance,
            TABLE_NAME,
            {"use_iceberg_partition_pruning": 0},
            {"use_iceberg_partition_pruning": 1},
            "IcebergPartitionPrunedFiles",
            select_expression,
        )

    assert prunned_files(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == 0
    assert prunned_files(f"SELECT * FROM {TABLE_NAME} WHERE price_a < 0 ORDER BY ALL") == 1
    assert prunned_files(f"SELECT * FROM {TABLE_NAME} WHERE price_b > 3 ORDER BY ALL") == 1


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_decimal_wide_minmax_pruning(started_cluster_iceberg_with_spark, storage_type):
    """Min/max statistics of `Decimal128` and `Decimal256` columns must be consumable by the reader.

    The bounds are longer than 8 bytes, which the bound deserializer used to reject, silently
    disabling `IcebergMinMaxIndexPrunedFiles` for these widths. `control` carries the same
    magnitudes in a type whose bounds are known to prune, to tell a decimal-specific gap apart
    from a table that simply cannot be pruned.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_decimal_wide_minmax_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(d128 Decimal(38, 10), d256 Decimal(76, 20), control Int64)",
        2,
    )

    # One file per insert, so the min/max bounds of every file cover exactly one row.
    for d128, d256, control in [
        ("1.5", "1.5", "1"),
        ("9999999999999999999999999999.5", "99999999999999999999999999999999999999999999999999999999.5", "500"),
        ("-9999999999999999999999999999.5", "-99999999999999999999999999999999999999999999999999999999.5", "-500"),
    ]:
        instance.query(
            f"INSERT INTO {TABLE_NAME} VALUES ({d128}, {d256}, {control})",
            settings={"allow_insert_into_iceberg": 1},
        )

    base_settings = {
        "input_format_parquet_bloom_filter_push_down": 0,
        "input_format_parquet_filter_push_down": 0,
        "output_format_decimal_trailing_zeros": 1,
    }

    def measure(predicate, index):
        query = f"SELECT * FROM {TABLE_NAME} {predicate} ORDER BY ALL"
        without_pruning = instance.query(
            query, settings={**base_settings, "use_iceberg_partition_pruning": 0}
        )
        query_id = f"{TABLE_NAME}-{index}"
        with_pruning = instance.query(
            query,
            query_id=query_id,
            settings={**base_settings, "use_iceberg_partition_pruning": 1},
        )
        instance.query("SYSTEM FLUSH LOGS")
        pruned = instance.query(
            f"SELECT ProfileEvents['IcebergMinMaxIndexPrunedFiles'] FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        ).strip()
        return without_pruning, with_pruning, int(pruned or 0)

    predicates = [
        "",
        "WHERE control < -100",
        "WHERE d128 > 1000000",
        "WHERE d128 < -1000000",
        "WHERE d256 > 1000000",
        "WHERE d256 < -1000000",
    ]
    measured = {
        predicate: measure(predicate, index) for index, predicate in enumerate(predicates)
    }

    # Pruning must never change the result, whatever it manages to skip.
    for predicate, (without_pruning, with_pruning, _) in measured.items():
        assert without_pruning == with_pruning, predicate

    assert measured[""][2] == 0
    assert measured["WHERE control < -100"][2] == 2
    assert measured["WHERE d128 > 1000000"][2] == 2
    assert measured["WHERE d128 < -1000000"][2] == 2
    assert measured["WHERE d256 > 1000000"][2] == 2
    assert measured["WHERE d256 < -1000000"][2] == 2
