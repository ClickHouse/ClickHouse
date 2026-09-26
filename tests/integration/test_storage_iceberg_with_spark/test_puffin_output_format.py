import os

from helpers.iceberg_utils import get_uuid_str

USER_FILES = "/var/lib/clickhouse/user_files"
ICEBERG_ROW_POSITION_FIELD_ID = 2147483645


def write_puffin(instance, relative_path, referenced_data_file, positions_sql):
    instance.query(
        f"""
        INSERT INTO FUNCTION file('{relative_path}', Puffin)
        SELECT arrayJoin({positions_sql}) AS position
        SETTINGS output_format_puffin_referenced_data_file = '{referenced_data_file}'
        """
    )
    offset, length, cardinality = (
        instance.query(
            f"SELECT offset, length, properties['cardinality'] FROM file('{relative_path}', PuffinMetadata)"
        )
        .strip()
        .split("\t")
    )
    container_path = f"{USER_FILES}/{relative_path}"
    os.makedirs(os.path.dirname(container_path), exist_ok=True)
    instance.copy_file_from_container(container_path, container_path)
    return container_path, int(offset), int(length), int(cardinality)


def build_delete_file(jvm, spec, puffin_path, referenced_data_file, offset, length, cardinality):
    return (
        jvm.org.apache.iceberg.FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withFormat("puffin")
        .withPath(puffin_path)
        .withFileSizeInBytes(os.path.getsize(puffin_path))
        .withRecordCount(cardinality)
        .withReferencedDataFile(referenced_data_file)
        .withContentOffset(offset)
        .withContentSizeInBytes(length)
        .build()
    )


def read_deletion_vector_with_iceberg(jvm, puffin_path, delete_file, referenced_data_file, cardinality):
    reader = jvm.org.apache.iceberg.puffin.Puffin.read(jvm.org.apache.iceberg.Files.localInput(puffin_path)).build()
    try:
        file_metadata = reader.fileMetadata()
        assert file_metadata.properties().get("created-by").startswith("ClickHouse ")
        blobs = file_metadata.blobs()
        assert blobs.size() == 1
        blob = blobs.get(0)
        assert blob.type() == "deletion-vector-v1"
        assert list(blob.inputFields()) == [ICEBERG_ROW_POSITION_FIELD_ID]
        assert blob.snapshotId() == -1
        assert blob.sequenceNumber() == -1
        assert blob.offset() == delete_file.contentOffset()
        assert blob.length() == delete_file.contentSizeInBytes()
        assert blob.compressionCodec() is None
        assert blob.properties().get("referenced-data-file") == referenced_data_file
        assert blob.properties().get("cardinality") == str(cardinality)

        iterator = reader.readAll(blobs).iterator()
        assert iterator.hasNext()
        blob_bytes = jvm.org.apache.iceberg.util.ByteBuffers.toByteArray(iterator.next().second())
        assert not iterator.hasNext()
    finally:
        reader.close()

    index = jvm.org.apache.iceberg.deletes.PositionDeleteIndex.deserialize(blob_bytes, delete_file)
    assert index.cardinality() == cardinality
    return index


def test_spark_reads_deletion_vector_written_by_clickhouse(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_puffin_output_format_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {table_name} (id bigint, data string) USING iceberg
        TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {table_name} SELECT id, cast(id AS string) FROM range(0, 100000, 1, 1)")

    data_files = [row["file_path"] for row in spark.sql(f"SELECT file_path FROM default.{table_name}.files").collect()]
    assert len(data_files) == 1
    data_file = data_files[0]

    deleted = [
        0, 1, 7, 100, 65535, 65536, 65537, 99999,
        2147483647, 2147483648, 4294967295, 4294967296, 4294967297,
        6442450944, 8589934591, 8589934592, 12884901888, 17179869183,
    ]
    deleted_in_file = [position for position in deleted if position < 100000]
    puffin_path, offset, length, cardinality = write_puffin(
        instance,
        f"iceberg_data/default/{table_name}/data/{get_uuid_str()}-deletes.puffin",
        data_file,
        f"{deleted}::Array(UInt64)",
    )
    assert cardinality == len(deleted)

    jvm = spark._jvm
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table_name)
    delete_file = build_delete_file(jvm, table.spec(), puffin_path, data_file, offset, length, cardinality)
    table.newRowDelta().addDeletes(delete_file).commit()
    spark.sql(f"REFRESH TABLE {table_name}")

    ids = [row["id"] for row in spark.sql(f"SELECT id FROM {table_name} ORDER BY id").collect()]
    assert ids == [i for i in range(100000) if i not in set(deleted)]
    assert spark.sql(f"SELECT count(*) FROM {table_name}").collect()[0][0] == 100000 - len(deleted_in_file)
    assert [row["id"] for row in spark.sql(f"SELECT id FROM {table_name} WHERE id < 10 ORDER BY id").collect()] == [2, 3, 4, 5, 6, 8, 9]
    assert [row["id"] for row in spark.sql(f"SELECT id FROM {table_name} WHERE id >= 65530 AND id < 65540 ORDER BY id").collect()] == [
        65530, 65531, 65532, 65533, 65534, 65538, 65539,
    ]

    delete_files = spark.sql(
        f"SELECT content, file_format, record_count, referenced_data_file, content_offset, content_size_in_bytes FROM default.{table_name}.delete_files"
    ).collect()
    assert len(delete_files) == 1
    assert delete_files[0]["content"] == 1
    assert delete_files[0]["file_format"] == "PUFFIN"
    assert delete_files[0]["record_count"] == len(deleted)
    assert delete_files[0]["referenced_data_file"] == data_file
    assert delete_files[0]["content_offset"] == offset
    assert delete_files[0]["content_size_in_bytes"] == length

    index = read_deletion_vector_with_iceberg(jvm, puffin_path, delete_file, data_file, cardinality)
    for position in deleted:
        assert index.isDeleted(position)
    for position in [
        2, 6, 8, 99, 101, 65534, 65538, 99998,
        2147483646, 2147483649, 4294967294, 4294967298, 6442450943, 6442450945,
        8589934590, 8589934593, 12884901887, 12884901889, 17179869182,
    ]:
        assert not index.isDeleted(position)


def test_iceberg_reads_multi_bitmap_deletion_vector_written_by_clickhouse(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    jvm = spark._jvm

    referenced_data_file = f"{USER_FILES}/iceberg_data/default/nonexistent_{get_uuid_str()}/data/00000-0-data.parquet"
    puffin_path, offset, length, cardinality = write_puffin(
        instance,
        f"iceberg_data/{get_uuid_str()}-deletes.puffin",
        referenced_data_file,
        "arrayConcat(range(toUInt64(1000), toUInt64(200000)), [4294967301, 12884901888]::Array(UInt64))",
    )
    assert cardinality == 199000 + 2

    delete_file = build_delete_file(
        jvm, jvm.org.apache.iceberg.PartitionSpec.unpartitioned(), puffin_path, referenced_data_file, offset, length, cardinality
    )
    index = read_deletion_vector_with_iceberg(jvm, puffin_path, delete_file, referenced_data_file, cardinality)
    for position in [1000, 123456, 199999, 4294967301, 12884901888]:
        assert index.isDeleted(position)
    for position in [0, 999, 200000, 4294967300, 4294967302, 12884901887, 12884901889]:
        assert not index.isDeleted(position)
