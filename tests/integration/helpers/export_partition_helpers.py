"""
Shared helpers for export-partition and export-part integration tests.

Centralises wait-polling, table creation, and partition helpers that were
previously duplicated across multiple test modules.
"""

import time
import uuid


MINIO_USER = "minio"
MINIO_PASS = "ClickHouse_Minio_P@ssw0rd"


def wait_for_export_status(
    node,
    source_table,
    dest_table,
    partition_id,
    expected_status="COMPLETED",
    timeout=60,
    poll_interval=0.5,
):
    """Poll system.replicated_partition_exports until status matches.

    *dest_table* may be ``None`` to skip filtering by destination table
    (useful for catalog-based tests where the destination is a database-qualified path).
    """
    start_time = time.time()
    last_status = None
    while time.time() - start_time < timeout:
        dest_filter = (
            f" AND destination_table = '{dest_table}'" if dest_table else ""
        )
        status = node.query(
            f"SELECT status FROM system.replicated_partition_exports"
            f" WHERE source_table = '{source_table}'"
            f"{dest_filter}"
            f" AND partition_id = '{partition_id}'"
        ).strip()

        last_status = status
        if status and status == expected_status:
            return status

        time.sleep(poll_interval)

    raise TimeoutError(
        f"Export status did not reach '{expected_status}' within {timeout}s. "
        f"Last status: '{last_status}'"
    )


def wait_for_export_to_start(
    node,
    source_table,
    dest_table,
    partition_id,
    timeout=10,
    poll_interval=0.2,
):
    """Poll until at least one row exists in system.replicated_partition_exports."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        count = node.query(
            f"SELECT count() FROM system.replicated_partition_exports"
            f" WHERE source_table = '{source_table}'"
            f"   AND destination_table = '{dest_table}'"
            f"   AND partition_id = '{partition_id}'"
        ).strip()

        if count != "0":
            return True

        time.sleep(poll_interval)

    raise TimeoutError(
        f"Export of partition {partition_id!r} did not start within {timeout}s."
    )


def wait_for_exception_count(
    node,
    source_table,
    dest_table,
    partition_id,
    min_exception_count=1,
    timeout=30,
    poll_interval=0.5,
):
    """Wait for exception_count to reach at least *min_exception_count*."""
    start_time = time.time()
    last_exception_count = None
    while time.time() - start_time < timeout:
        exception_count_str = node.query(
            f"SELECT exception_count FROM system.replicated_partition_exports"
            f" WHERE source_table = '{source_table}'"
            f"   AND destination_table = '{dest_table}'"
            f"   AND partition_id = '{partition_id}'"
            f" SETTINGS export_merge_tree_partition_system_table_prefer_remote_information = 1"
        ).strip()

        if exception_count_str:
            exception_count = int(exception_count_str)
            last_exception_count = exception_count
            if exception_count >= min_exception_count:
                return exception_count

        time.sleep(poll_interval)

    raise TimeoutError(
        f"Exception count did not reach {min_exception_count} within {timeout}s. "
        f"Last exception_count: {last_exception_count if last_exception_count is not None else 'N/A'}"
    )


# -- block-number settings are needed for patch parts support
_BLOCK_SETTINGS = (
    "enable_block_number_column = 1, enable_block_offset_column = 1"
)


def make_rmt(
    node,
    name,
    columns,
    partition_by,
    replica_name="r1",
    order_by="tuple()",
):
    """Create a ReplicatedMergeTree table with block-number settings."""
    node.query(
        f"""
        CREATE TABLE {name} ({columns})
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{name}', '{replica_name}')
        PARTITION BY {partition_by}
        ORDER BY {order_by}
        SETTINGS {_BLOCK_SETTINGS}
        """
    )


def make_mt(
    node,
    name,
    columns,
    partition_by,
    order_by="tuple()",
):
    """Create a MergeTree table with block-number settings."""
    node.query(
        f"""
        CREATE TABLE {name} ({columns})
        ENGINE = MergeTree()
        PARTITION BY {partition_by}
        ORDER BY {order_by}
        SETTINGS {_BLOCK_SETTINGS}
        """
    )


def make_iceberg_s3(
    node,
    name,
    columns,
    partition_by="",
    url=None,
    s3_retry_attempts=3,
    if_not_exists=False,
):
    """Create an IcebergS3 table at a MinIO prefix.

    *url* defaults to ``http://minio1:9001/root/data/{name}/``.
    """
    if url is None:
        url = f"http://minio1:9001/root/data/{name}/"
    ine = "IF NOT EXISTS " if if_not_exists else ""
    pclause = f"PARTITION BY {partition_by}" if partition_by else ""
    node.query(
        f"""
        CREATE TABLE {ine}{name} ({columns})
        ENGINE = IcebergS3('{url}', '{MINIO_USER}', '{MINIO_PASS}')
        {pclause}
        SETTINGS s3_retry_attempts = {s3_retry_attempts}
        """
    )


def first_partition_id(node, table):
    """Return the partition_id of the first active part of *table*."""
    return node.query(
        f"SELECT partition_id FROM system.parts"
        f" WHERE database = currentDatabase() AND table = '{table}' AND active"
        f" ORDER BY name LIMIT 1"
    ).strip()


def unique_suffix():
    """Return a UUID with hyphens replaced by underscores, suitable for table names."""
    return str(uuid.uuid4()).replace("-", "_")
