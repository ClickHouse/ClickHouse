import glob

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
    default_upload_directory,
)


# Regression test for https://github.com/ClickHouse/ClickHouse/issues/109988.
# ClickHouse-written Avro/ORC Iceberg data files must embed Iceberg field IDs
# (Avro field-id, ORC iceberg.id) and ORC must store String columns as ORC
# string. Without them, iceberg-java falls back to name matching: after a
# RENAME COLUMN, Spark silently returns NULL for ClickHouse-written rows
# (Avro), or fails to read the table at all (ORC binary vs string). With the
# field IDs present, Spark projects by ID and reads the renamed column
# correctly - which is what this test asserts.
@pytest.mark.parametrize("format", ["Avro", "ORC"])
def test_writes_field_ids_spark_read_after_rename(
    started_cluster_iceberg_with_spark, format
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_field_ids_spark_read_" + format + "_" + get_uuid_str()
    local_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int32, label String, score Float64)",
        2,
        format=format,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'alice', 1.5), (2, 'bob', 2.5), (3, 'charlie', 3.5)",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert (
        instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id")
        == "1\talice\t1.5\n2\tbob\t2.5\n3\tcharlie\t3.5\n"
    )

    # Rename the String column. Spark must still read the ClickHouse-written
    # values for the renamed column by projecting via the file field IDs.
    instance.query(
        f"ALTER TABLE {TABLE_NAME} RENAME COLUMN label TO name",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert (
        instance.query(f"SELECT id, name, score FROM {TABLE_NAME} ORDER BY id")
        == "1\talice\t1.5\n2\tbob\t2.5\n3\tcharlie\t3.5\n"
    )

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"{local_path}/",
        f"{local_path}/",
    )

    rows = spark.read.format("iceberg").load(local_path).orderBy("id").collect()

    assert len(rows) == 3
    # The renamed column must carry the ClickHouse-written values, not NULL.
    assert [(r.id, r.name, r.score) for r in rows] == [
        (1, "alice", 1.5),
        (2, "bob", 2.5),
        (3, "charlie", 3.5),
    ]


def _avro_field_ids(node_schema):
    """Collect every {path: field-id} pair from an avro record schema tree,
    descending through array/map wrappers exactly like the writer does."""
    import avro.schema

    result = {}

    def walk(schema, path):
        if isinstance(schema, avro.schema.RecordSchema):
            for field in schema.fields:
                child_path = f"{path}.{field.name}" if path else field.name
                fid = field.other_props.get("field-id")
                if fid is not None:
                    result[child_path] = fid
                walk(field.type, child_path)
        elif isinstance(schema, avro.schema.UnionSchema):
            for member in schema.schemas:
                walk(member, path)
        elif isinstance(schema, avro.schema.ArraySchema):
            walk(schema.items, f"{path}.element")
        elif isinstance(schema, avro.schema.MapSchema):
            walk(schema.values, f"{path}.value")

    walk(node_schema, "")
    return result


# Regression for the wrapped-nested Avro case (bot review on #109994): the Avro
# field-id writer must descend through array / union wrappers to reach a record
# nested inside e.g. Array(Tuple(...)). The previous writer stopped at the first
# non-record node, so a struct wrapped in an array received no nested field-ids
# and iceberg-java would fall back to name matching for those subfields. This
# test writes such a column from ClickHouse and asserts the nested subfield IDs
# are present in the Avro data file (and that Spark can read the file back).
def test_writes_field_ids_nested_avro(started_cluster_iceberg_with_spark):
    import avro.datafile
    import avro.io

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_field_ids_nested_avro_" + get_uuid_str()
    local_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int32, info Array(Tuple(a Int32, b String)))",
        2,
        format="Avro",
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, [(10, 'x'), (11, 'y')]), (2, [(20, 'z')])",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert (
        instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY id")
        == "1\t[(10,'x'),(11,'y')]\n2\t[(20,'z')]\n"
    )

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"{local_path}/",
        f"{local_path}/",
    )

    data_files = glob.glob(f"{local_path}/data/*.avro")
    assert data_files, "no Avro data file was written"

    with open(data_files[0], "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        writer_schema = reader.datum_reader.writers_schema
        reader.close()

    field_ids = _avro_field_ids(writer_schema)
    # The nested struct subfields must carry field-ids reached through the array
    # wrapper. Without the wrapper recursion these keys would be absent.
    assert "info.element.a" in field_ids, field_ids
    assert "info.element.b" in field_ids, field_ids
    assert field_ids["info.element.a"] != field_ids["info.element.b"]

    rows = spark.read.format("iceberg").load(local_path).orderBy("id").collect()
    assert [(r.id, [(e.a, e.b) for e in r.info]) for r in rows] == [
        (1, [(10, "x"), (11, "y")]),
        (2, [(20, "z")]),
    ]


# Regression for the Avro Map(String, Tuple(...)) case (bot review on #109994):
# the Avro field-id writer must descend into a map's value schema to reach a
# record nested inside e.g. Map(String, Tuple(a Int32, b String)), so paths like
# `m.value.a` get field-ids. This asserts the nested map-value subfield IDs are
# present in the Avro data file (and that Spark reads the map back).
def test_writes_field_ids_map_tuple_avro(started_cluster_iceberg_with_spark):
    import avro.datafile
    import avro.io

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_field_ids_map_tuple_avro_" + get_uuid_str()
    local_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int32, m Map(String, Tuple(a Int32, b String)))",
        2,
        format="Avro",
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, {{'k1': (10, 'x'), 'k2': (11, 'y')}}), (2, {{'k3': (20, 'z')}})",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == "1\n2\n"

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"{local_path}/",
        f"{local_path}/",
    )

    data_files = glob.glob(f"{local_path}/data/*.avro")
    assert data_files, "no Avro data file was written"

    with open(data_files[0], "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        writer_schema = reader.datum_reader.writers_schema
        reader.close()

    field_ids = _avro_field_ids(writer_schema)
    # The nested map-value struct subfields must carry field-ids reached through
    # the map wrapper. Without the map-value recursion these keys would be absent.
    assert "m.value.a" in field_ids, field_ids
    assert "m.value.b" in field_ids, field_ids
    assert field_ids["m.value.a"] != field_ids["m.value.b"]

    rows = spark.read.format("iceberg").load(local_path).orderBy("id").collect()
    assert len(rows) == 2
    assert {k: (v.a, v.b) for k, v in rows[0].m.items()} == {
        "k1": (10, "x"),
        "k2": (11, "y"),
    }
    assert {k: (v.a, v.b) for k, v in rows[1].m.items()} == {"k3": (20, "z")}


# Regression for the ORC string-vs-binary scoping (bot review on #109994):
# IcebergSchemaProcessor::getSimpleType maps BOTH Iceberg `string` and Iceberg
# `binary` to ClickHouse DataTypeString, so keying the ORC String -> ORC `string`
# override on "column_mapper present" alone would corrupt an Iceberg `binary`
# column into ORC `string`, changing its logical type. The writer must consult
# the per-path source Iceberg logical type: force ORC `string` only for genuine
# Iceberg `string` fields and keep Iceberg `binary` as ORC `binary`.
#
# CH's own getIcebergType never emits `binary`, so the `binary` field must come
# from a Spark-created table. We register it in ClickHouse, INSERT (ClickHouse
# writes the ORC data file with the Iceberg mapper), then read the raw ORC file
# with pyarrow and assert the physical ORC types: `s` -> arrow string (ORC
# string), `b` -> arrow binary (ORC binary, NOT string).
def test_writes_field_ids_orc_binary_vs_string(started_cluster_iceberg_with_spark):
    import pyarrow.orc

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_field_ids_orc_binary_vs_string_" + get_uuid_str()
    local_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    # Spark creates the table so it has a genuine Iceberg `binary` field alongside
    # a `string` field (both read as ClickHouse String).
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id INT, s STRING, b BINARY)
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.format.default'='orc')
        """
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        format="ORC",
    )

    # ClickHouse writes the ORC data file (with the Iceberg column mapper). With
    # output_format_orc_string_as_string=0 to prove the string/binary choice is
    # driven by the source Iceberg type, not the session setting.
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'hello', unhex('DEADBEEF'))",
        settings={
            "allow_insert_into_iceberg": 1,
            "output_format_orc_string_as_string": 0,
        },
    )

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"{local_path}/",
        f"{local_path}/",
    )

    data_files = glob.glob(f"{local_path}/data/*.orc")
    assert data_files, "no ORC data file was written"

    orc_schema = pyarrow.orc.ORCFile(data_files[0]).schema
    field_types = {orc_schema.field(i).name: orc_schema.field(i).type for i in range(len(orc_schema))}
    # Iceberg `string` -> ORC string (arrow string); Iceberg `binary` -> ORC
    # binary (arrow binary). If the mapper-presence heuristic leaked, `b` would
    # be arrow string here.
    import pyarrow
    assert pyarrow.types.is_string(field_types["s"]), field_types
    assert pyarrow.types.is_binary(field_types["b"]), field_types

    # Spark must still read the ClickHouse-written values back correctly.
    rows = spark.read.format("iceberg").load(local_path).orderBy("id").collect()
    assert [(r.id, r.s, bytes(r.b)) for r in rows] == [(1, "hello", bytes.fromhex("DEADBEEF"))]


def _avro_data_file_schema(path):
    """Return the writer schema of the first Avro data file under path/data/."""
    import avro.datafile
    import avro.io

    data_files = glob.glob(f"{path}/data/*.avro")
    assert data_files, "no Avro data file was written"
    with open(data_files[0], "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        return reader.datum_reader.writers_schema


# Regression for the Avro string-vs-binary scoping (bot review on #109994):
# IcebergSchemaProcessor::getSimpleType maps BOTH Iceberg `string` and Iceberg
# `binary` to ClickHouse DataTypeString. The Avro writer chose `string` vs
# `bytes` from the output_format_avro_string_column_pattern regex, which cannot
# tell the two apart, so it could emit Avro `string` for a `binary` field or
# `bytes` for a `string` field. On the Iceberg path the writer must instead pick
# from the per-path source Iceberg logical type: `string` -> Avro string,
# `binary` -> Avro bytes. The `binary` field must come from a Spark-created
# table (CH's getIcebergType never emits `binary`).
def test_writes_field_ids_avro_binary_vs_string(started_cluster_iceberg_with_spark):
    import avro.schema

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_field_ids_avro_binary_vs_string_" + get_uuid_str()
    local_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id INT, s STRING, b BINARY)
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.format.default'='avro')
        """
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        format="Avro",
    )

    # Choose a string_column_pattern that would (wrongly) force the `binary`
    # column `b` to Avro string and miss the `string` column `s`, if the writer
    # still used the regex. The per-path logical type must override it.
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'hello', unhex('DEADBEEF'))",
        settings={
            "allow_insert_into_iceberg": 1,
            "output_format_avro_string_column_pattern": "b",
        },
    )

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"{local_path}/",
        f"{local_path}/",
    )

    schema = _avro_data_file_schema(local_path)

    def _avro_leaf_type(field_schema):
        # Iceberg fields are nullable, i.e. union [null, T]; unwrap to T.
        if isinstance(field_schema, avro.schema.UnionSchema):
            for member in field_schema.schemas:
                if member.type != "null":
                    return member.type
        return field_schema.type

    field_types = {f.name: _avro_leaf_type(f.type) for f in schema.fields}
    # Iceberg `string` -> Avro string; Iceberg `binary` -> Avro bytes. If the
    # regex leaked, `b` would be "string" and `s` would be "bytes" here.
    assert field_types["s"] == "string", field_types
    assert field_types["b"] == "bytes", field_types

    rows = spark.read.format("iceberg").load(local_path).orderBy("id").collect()
    assert [(r.id, r.s, bytes(r.b)) for r in rows] == [(1, "hello", bytes.fromhex("DEADBEEF"))]
