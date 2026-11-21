import glob

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
)


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


def _avro_metadata_schemas(path):
    """Return (manifest_list_schema, [manifest_schemas]) writer schemas from the
    Avro files under path/metadata/. Manifest lists are the snap-*.avro files;
    the remaining *.avro files are manifests."""
    import avro.datafile
    import avro.io

    def _writer_schema(avro_path):
        with open(avro_path, "rb") as f:
            reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            return reader.datum_reader.writers_schema

    manifest_lists = glob.glob(f"{path}/metadata/snap-*.avro")
    manifests = [
        p
        for p in glob.glob(f"{path}/metadata/*.avro")
        if "/snap-" not in p and not p.rsplit("/", 1)[-1].startswith("snap-")
    ]
    assert manifest_lists, "no manifest-list (snap-*.avro) was written"
    assert manifests, "no manifest (*.avro) was written"
    return _writer_schema(manifest_lists[0]), [_writer_schema(p) for p in manifests]


# Regression test for https://github.com/ClickHouse/ClickHouse/issues/111763.
# The Avro schemas embedded in ClickHouse-written manifest-list and manifest
# files must carry the Iceberg spec field-ids (manifest_path=500, status=0,
# data_file=2, ...). The bundled avro-cpp JSON compiler drops the field-id/
# element-id attributes, so before the fix the header schema serialized by
# DataFileWriter omitted them and PyIceberg/Spark could not plan a scan of a
# ClickHouse-written table (ValueError: Cannot convert field, missing field-id).
# ClickHouse itself reads via the Iceberg `schema` metadata key, so a ClickHouse
# round-trip cannot catch this - an external reader (Spark here) is required.
def test_writes_manifest_field_ids_spark_read(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_manifest_field_ids_" + get_uuid_str()
    local_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    # Partitioned so the manifest `data_file.partition` struct is populated and
    # its field-id must match the persisted partition spec.
    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int32, label String, score Float64)",
        2,
        partition_by="id",
        format="Avro",
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'alice', 1.5), (2, 'bob', 2.5), (3, 'charlie', 3.5)",
        settings={"allow_insert_into_iceberg": 1},
    )

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"{local_path}/",
        f"{local_path}/",
    )

    manifest_list_schema, manifest_schemas = _avro_metadata_schemas(local_path)

    # Manifest-list (`manifest_file`) spec field-ids.
    ml_ids = _avro_field_ids(manifest_list_schema)
    assert ml_ids.get("manifest_path") == 500, ml_ids
    assert ml_ids.get("manifest_length") == 501, ml_ids
    assert ml_ids.get("partition_spec_id") == 502, ml_ids
    assert ml_ids.get("added_snapshot_id") == 503, ml_ids
    # `partitions` is a list; its field-summary subfields carry ids too.
    assert ml_ids.get("partitions") == 507, ml_ids
    assert ml_ids.get("partitions.element.contains_null") == 509, ml_ids

    # The partition spec field-id the manifest `partition` struct must reuse. The
    # single partition column `id` is field-id 1 in the schema, so the spec assigns
    # it partition field-id 1001 (ClickHouse numbers partition fields from 1001).
    expected_partition_field_id = 1001

    # Manifest (`manifest_entry`) spec field-ids, on every manifest written.
    for manifest_schema in manifest_schemas:
        m_ids = _avro_field_ids(manifest_schema)
        assert m_ids.get("status") == 0, m_ids
        assert m_ids.get("snapshot_id") == 1, m_ids
        assert m_ids.get("data_file") == 2, m_ids
        assert m_ids.get("data_file.file_path") == 100, m_ids
        assert m_ids.get("data_file.record_count") == 103, m_ids
        # The map subfields (column_sizes etc.) are Avro logicalType=map arrays;
        # their key/value carry Iceberg key-id/value-id, reached via `.element`.
        assert m_ids.get("data_file.column_sizes.element.key") == 117, m_ids
        assert m_ids.get("data_file.column_sizes.element.value") == 118, m_ids
        # The manifest `partition` struct field-id must equal the partition spec's.
        assert m_ids.get("data_file.partition.id") == expected_partition_field_id, m_ids

    # End-to-end: Spark plans the scan via the manifest field-ids (exactly what
    # was broken) and reads the ClickHouse-written rows back.
    rows = spark.read.format("iceberg").load(local_path).orderBy("id").collect()
    assert [(r.id, r.label, r.score) for r in rows] == [
        (1, "alice", 1.5),
        (2, "bob", 2.5),
        (3, "charlie", 3.5),
    ]
