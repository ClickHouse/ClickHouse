import json

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    generate_data,
    get_uuid_str,
    write_iceberg_from_df,
)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_iceberg_history_summary_from_spark(
    started_cluster_iceberg_with_spark, format_version, storage_type
):
    """Snapshots written by Spark must be read back correctly through
    `system.iceberg_history`: the `operation` column and the `summary` map are parsed from
    the Iceberg metadata Spark produced. Exercises every operation Spark emits."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = (
        "test_iceberg_history_summary_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    # APPEND (create) + APPEND -> two data files.
    write_iceberg_from_df(spark, generate_data(spark, 0, 100), table_name, mode="overwrite", format_version=format_version)
    write_iceberg_from_df(spark, generate_data(spark, 100, 150), table_name, mode="append", format_version=format_version)
    # REPLACE: compact the two files into one.
    spark.sql(
        f"CALL spark_catalog.system.rewrite_data_files("
        f"table => 'default.{table_name}', options => map('min-input-files','2'))"
    )
    # OVERWRITE: replace the whole (unpartitioned) table.
    spark.sql(f"INSERT OVERWRITE {table_name} VALUES (1, '1')")
    # DELETE: remove all remaining rows.
    spark.sql(f"DELETE FROM {table_name} WHERE a >= 0")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )

    create_iceberg_table(storage_type, instance, table_name, started_cluster_iceberg_with_spark)

    rows = (
        instance.query(
            f"SELECT operation, toJSONString(summary) "
            f"FROM system.iceberg_history "
            f"WHERE database = 'default' AND table = '{table_name}' "
            f"ORDER BY made_current_at FORMAT TSV"
        )
        .strip()
        .split("\n")
    )
    history = [(op, json.loads(summary)) for op, summary in (row.split("\t", 1) for row in rows)]

    # Five snapshots: append, append, replace, overwrite, delete.
    operations = [op for op, _ in history]
    assert operations == ["APPEND", "APPEND", "REPLACE", "OVERWRITE", "DELETE"], operations

    # Every snapshot must carry Spark's engine markers through the summary map.
    for op, summary in history:
        assert summary.get("engine-name") == "spark", (op, summary)
        assert summary.get("engine-version"), (op, summary)
        assert summary.get("iceberg-version"), (op, summary)

    by_op = {op: summary for op, summary in history}

    # APPEND of 100 rows is the first snapshot in execution order.
    first_append = history[0][1]
    assert first_append["added-records"] == "100"
    assert first_append["added-data-files"] == "1"
    assert first_append["total-records"] == "100"

    # REPLACE (compaction): rewrote 2 files into 1, same 150 records.
    replace = by_op["REPLACE"]
    assert replace["added-records"] == "150"
    assert replace["deleted-data-files"] == "2"
    assert replace["deleted-records"] == "150"
    assert replace["total-records"] == "150"

    # OVERWRITE: replaced 150 rows with a single row.
    overwrite = by_op["OVERWRITE"]
    assert overwrite["added-records"] == "1"
    assert overwrite["deleted-records"] == "150"
    assert overwrite["deleted-data-files"] == "1"
    assert overwrite["total-records"] == "1"

    # DELETE: removed the last remaining row, table now empty.
    delete = by_op["DELETE"]
    assert delete["deleted-records"] == "1"
    assert delete["deleted-data-files"] == "1"
    assert delete["total-records"] == "0"
    assert delete["total-data-files"] == "0"
