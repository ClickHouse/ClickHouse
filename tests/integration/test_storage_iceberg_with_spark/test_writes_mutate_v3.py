import glob

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_uuid_str,
)

SETTINGS = {"allow_insert_into_iceberg": 1, "mutations_sync": 1}


def manifest_content_headers(spark, table_path):
    """The `content` header of every manifest of the table, read with Avro Java, independent of ClickHouse."""
    jvm = spark._jvm
    headers = {}
    for path in glob.glob(f"{table_path}/metadata/*.avro"):
        reader = jvm.org.apache.avro.file.DataFileReader(
            jvm.java.io.File(path), jvm.org.apache.avro.generic.GenericDatumReader()
        )
        try:
            if reader.getMetaString("partition-spec") is not None:
                headers[path] = reader.getMetaString("content")
        finally:
            reader.close()
    return headers


@pytest.mark.parametrize("partition_by", ["", "identity(p)"])
def test_spark_reads_table_after_clickhouse_delete_and_update(started_cluster_iceberg_with_spark, partition_by):
    storage_type = "s3"
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_writes_mutate_v3_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    create_iceberg_table(
        storage_type, instance, table_name, started_cluster_iceberg_with_spark,
        "(id Int32, p String, v String)", format_version=3, partition_by=partition_by,
    )
    instance.query(
        f"INSERT INTO {table_name} SELECT number, if(number % 2 = 0, 'even', 'odd'), toString(number) FROM numbers(20)",
        settings=SETTINGS,
    )
    row_ids_before = dict(
        tuple(map(int, line.split("\t")))
        for line in instance.query(f"SELECT id, _row_id FROM {table_name} FORMAT TSV").split("\n") if line
    )

    instance.query(f"DELETE FROM {table_name} WHERE id < 3", settings=SETTINGS)
    instance.query(f"DELETE FROM {table_name} WHERE id IN (7, 8)", settings=SETTINGS)
    instance.query(f"ALTER TABLE {table_name} UPDATE v = 'updated' WHERE id IN (4, 5, 15)", settings=SETTINGS)

    expected = [
        (i, "even" if i % 2 == 0 else "odd", "updated" if i in (4, 5, 15) else str(i))
        for i in range(20) if i >= 3 and i not in (7, 8)
    ]
    assert instance.query(f"SELECT id, p, v FROM {table_name} ORDER BY id FORMAT TSV") == "".join(
        f"{i}\t{p}\t{v}\n" for i, p, v in expected
    )

    default_download_directory(started_cluster_iceberg_with_spark, storage_type, f"{table_path}/", f"{table_path}/")

    rows = (
        spark.read.format("iceberg").load(table_path)
        .select("id", "p", "v", "_row_id").orderBy("id").collect()
    )
    assert [(row["id"], row["p"], row["v"]) for row in rows] == expected
    assert {row["id"]: row["_row_id"] for row in rows} == {i: row_ids_before[i] for i, _, _ in expected}

    delete_files = spark.read.format("iceberg").load(f"{table_path}#delete_files").select("file_format", "referenced_data_file").collect()
    assert delete_files and all(row["file_format"] == "PUFFIN" for row in delete_files)
    referenced = [row["referenced_data_file"] for row in delete_files]
    assert len(referenced) == len(set(referenced))

    headers = manifest_content_headers(spark, table_path)
    assert "deletes" in headers.values() and set(headers.values()) <= {"data", "deletes"}
