import logging
import pytest
import re

from helpers.iceberg_utils import (
    default_upload_directory,
    write_iceberg_from_df,
    generate_data,
    get_creation_expression,
    get_uuid_str,
)

from helpers.config_cluster import minio_secret_key


def parse_logged_file_stats(instance, query_ids):
    """
    Parse Iceberg file stats from server log, filtering by any of the given query_ids.
    Returns dict: {file_path: {"record_count": int, "file_size_in_bytes": int}}
    """
    stats = {}

    record_count_logs = instance.grep_in_log("Iceberg record_count for")
    for log_line in record_count_logs.splitlines():
        if not any(qid in log_line for qid in query_ids):
            continue
        match = re.search(r"Iceberg record_count for '([^']+)': (\d+)", log_line)
        if match:
            path, count = match.group(1), int(match.group(2))
            stats.setdefault(path, {})["record_count"] = count

    file_size_logs = instance.grep_in_log("Iceberg file_size_in_bytes for")
    for log_line in file_size_logs.splitlines():
        if not any(qid in log_line for qid in query_ids):
            continue
        match = re.search(
            r"Iceberg file_size_in_bytes for '([^']+)': (\d+)", log_line
        )
        if match:
            path, size = match.group(1), int(match.group(2))
            stats.setdefault(path, {})["file_size_in_bytes"] = size

    return stats


@pytest.mark.parametrize("run_on_cluster", [False, True])
@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_iceberg_file_stats_logging(
    started_cluster_iceberg_with_spark, format_version, storage_type, run_on_cluster
):
    """
    Verify that record_count and file_size_in_bytes parsed from Iceberg manifest
    match actual file row counts and sizes in object storage.
    When run_on_cluster=True, also verifies the values survive cluster function
    protocol serialization.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    bucket = started_cluster_iceberg_with_spark.minio_bucket

    cluster_suffix = "_cluster" if run_on_cluster else ""
    TABLE_NAME = (
        "test_file_stats_logging_"
        + format_version
        + "_"
        + storage_type
        + cluster_suffix
        + "_"
        + get_uuid_str()
    )

    NUM_ROWS = 100

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, NUM_ROWS),
        TABLE_NAME,
        mode="overwrite",
        format_version=format_version,
    )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    table_function_expr = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
        run_on_cluster=run_on_cluster,
    )

    query_id = TABLE_NAME + "_query_id"
    result = instance.query(
        f"SELECT * FROM {table_function_expr}", query_id=query_id
    )
    assert len(result.strip().split("\n")) == NUM_ROWS

    # Collect all query_ids related to this query across all nodes
    # (in cluster mode, worker nodes get their own query_id but share initial_query_id)
    all_query_ids = {query_id}
    for node_name, node_instance in started_cluster_iceberg_with_spark.instances.items():
        node_instance.query("SYSTEM FLUSH LOGS")
        related_ids = node_instance.query(
            f"SELECT query_id FROM system.query_log WHERE initial_query_id = '{query_id}'"
        ).strip()
        for qid in related_ids.splitlines():
            if qid:
                all_query_ids.add(qid)
    logging.info(f"All related query_ids: {all_query_ids}")

    # Collect file stats from all nodes, filtered by any related query_id
    all_stats = {}
    for node_name, node_instance in started_cluster_iceberg_with_spark.instances.items():
        node_stats = parse_logged_file_stats(node_instance, all_query_ids)
        logging.info(f"[{node_name}] Parsed file stats from logs: {node_stats}")
        all_stats.update(node_stats)

    assert len(all_stats) > 0, "Expected at least one file with logged stats"

    for file_path, file_stats in all_stats.items():
        assert "record_count" in file_stats, f"Missing record_count for {file_path}"
        assert (
            "file_size_in_bytes" in file_stats
        ), f"Missing file_size_in_bytes for {file_path}"

        logged_record_count = file_stats["record_count"]
        logged_file_size = file_stats["file_size_in_bytes"]

        # Verify record_count matches actual row count by reading the parquet file directly
        actual_row_count = int(
            instance.query(
                f"SELECT count() FROM s3('http://minio1:9001/{bucket}/{file_path}', 'minio', '{minio_secret_key}', 'Parquet')"
            ).strip()
        )
        assert logged_record_count == actual_row_count, (
            f"record_count mismatch for {file_path}: "
            f"logged={logged_record_count}, actual={actual_row_count}"
        )

        # Verify file_size_in_bytes matches actual object size in S3
        actual_size = started_cluster_iceberg_with_spark.minio_client.stat_object(
            bucket, file_path
        ).size
        assert logged_file_size == actual_size, (
            f"file_size_in_bytes mismatch for {file_path}: "
            f"logged={logged_file_size}, actual={actual_size}"
        )

    # Verify total record_count sums to at least NUM_ROWS
    total_record_count = sum(s.get("record_count", 0) for s in all_stats.values())
    assert total_record_count >= NUM_ROWS, (
        f"Total record_count ({total_record_count}) < expected ({NUM_ROWS})"
    )
