import functools
import glob
import json
import os
import re

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    default_upload_directory,
    get_uuid_str,
)

COLUMNS = "(id Int64, i Int32, s String, d Date, dec Decimal(18, 2), ts DateTime64(6), f Float64, b Bool)"


def values(start, count):
    return f"""
        SELECT number, toInt32(intDiv(number, 3)), if(number % 97 = 0, '', toString(intDiv(number, 5))), toDate('2020-01-01') + intDiv(number, 7),
            toDecimal64(intDiv(number, 2) / 4, 2), fromUnixTimestamp64Micro(toInt64(1600000000000000 + intDiv(number, 11))),
            intDiv(number, 13) / 8, number % 2 = 0
        FROM numbers({start}, {count})"""


def latest_metadata(local_path):
    files = glob.glob(os.path.join(local_path, "metadata", "*.metadata.json"))
    return max(files, key=lambda f: int(re.search(r"v(\d+)", os.path.basename(f)).group(1)))


def fix_version_hint_for_spark(local_path):
    """Spark's Hadoop catalog reads a plain version number from `version-hint.text`."""
    version = re.search(r"v(\d+)", os.path.basename(latest_metadata(local_path))).group(1)
    with open(os.path.join(local_path, "metadata", "version-hint.text"), "w") as f:
        f.write(version)


def current_statistics(local_path):
    """The path and the `ndv` of each column, by column name, of the statistics entry of the current snapshot."""
    with open(latest_metadata(local_path)) as f:
        metadata = json.load(f)
    schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
    names = {field["id"]: field["name"] for field in schema["fields"]}
    entry = next(e for e in metadata["statistics"] if e["snapshot-id"] == metadata["current-snapshot-id"])
    ndv = {names[blob["fields"][0]]: int(blob["properties"]["ndv"]) for blob in entry["blob-metadata"]}
    return metadata["current-snapshot-id"], entry["statistics-path"], ndv


def datasketches(jvm, name):
    """A DataSketches class, which the Iceberg Spark runtime may relocate."""
    for package in ["org.apache.datasketches", "org.apache.iceberg.shaded.org.apache.datasketches"]:
        try:
            jvm.java.lang.Class.forName(f"{package}.{name}")
        except Exception:
            continue
        return functools.reduce(getattr, f"{package}.{name}".split("."), jvm)
    raise AssertionError(f"DataSketches class {name} not found")


def spark_sketch_estimates(spark, table_name):
    """Read the theta sketches of the current snapshot through Iceberg's Puffin reader, by column name."""
    jvm = spark._jvm
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, f"default.{table_name}")
    snapshot_id = table.currentSnapshot().snapshotId()
    statistics = [f for f in table.statisticsFiles() if f.snapshotId() == snapshot_id]
    assert len(statistics) == 1
    reader = jvm.org.apache.iceberg.puffin.Puffin.read(table.io().newInputFile(statistics[0].path())).build()
    estimates = {}
    for blob in reader.fileMetadata().blobs():
        data = reader.readAll(jvm.java.util.Collections.singletonList(blob)).iterator().next().second()
        data = jvm.org.apache.iceberg.util.ByteBuffers.toByteArray(data)
        sketch = datasketches(jvm, "theta.CompactSketch").wrap(datasketches(jvm, "memory.Memory").wrap(data))
        estimates[table.schema().findColumnName(blob.inputFields().get(0))] = int(sketch.getEstimate())
    reader.close()
    return estimates


def spark_ndv(spark, local_path, table_name):
    """Spark's own statistics of the current snapshot, computed by a full scan, replacing ClickHouse's."""
    snapshot_id, clickhouse_path, _ = current_statistics(local_path)
    fix_version_hint_for_spark(local_path)
    spark.sql(f"REFRESH TABLE default.{table_name}")
    spark.sql(f"CALL spark_catalog.system.compute_table_stats(table => 'default.{table_name}', snapshot_id => {snapshot_id})")
    _, spark_path, ndv = current_statistics(local_path)
    assert spark_path != clickhouse_path
    return ndv


def test_writes_table_statistics_ndv_spark_interop(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    table_name = "test_writes_table_statistics_ndv_" + get_uuid_str()
    local_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"
    settings = {"allow_insert_into_iceberg": 1, "iceberg_write_table_statistics_ndv": 1}

    create_iceberg_table(storage_type, instance, table_name, started_cluster_iceberg_with_spark, COLUMNS, 2)
    instance.query(f"INSERT INTO {table_name} {values(0, 1000)}", settings=settings)

    # Spark reads ClickHouse's statistics file and its sketches give the NDVs ClickHouse recorded.
    default_download_directory(started_cluster_iceberg_with_spark, storage_type, local_path, local_path)
    _, _, clickhouse_ndv = current_statistics(local_path)
    fix_version_hint_for_spark(local_path)
    assert spark_sketch_estimates(spark, table_name) == clickhouse_ndv
    assert spark_ndv(spark, local_path, table_name) == clickhouse_ndv

    # The next ClickHouse insert overlaps the first one and unions with Spark's zstd-compressed sketches, so it matches
    # Spark's full scan only if both engines hash the same bytes for every type.
    default_upload_directory(started_cluster_iceberg_with_spark, storage_type, local_path, local_path)
    instance.query(f"INSERT INTO {table_name} {values(500, 1000)}", settings=settings)
    default_download_directory(started_cluster_iceberg_with_spark, storage_type, local_path, local_path)
    _, _, clickhouse_ndv = current_statistics(local_path)
    assert spark_ndv(spark, local_path, table_name) == clickhouse_ndv
