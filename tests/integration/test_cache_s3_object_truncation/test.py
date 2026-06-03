"""
Regression tests for graceful handling of truncated S3 objects during cached reads.

When an S3 object is overwritten with shorter content between listing and
reading, the cache layer must NOT throw LOGICAL_ERROR. Instead it should
detect the truncation and either:
  - switch to bypass-cache mode (predownloadForFileSegment path), or
  - return a short read (readBigAt path), or
  - treat it as legitimate EOF (readFromFileSegment path).

NOTE: the MergeTree-based tests below exercise the sequential cached-read
paths (predownloadForFileSegment / readFromFileSegment). `readBigAt` is the
random-access path used by Parquet over object storage; it is NOT reached by
a MergeTree SELECT and would need a separate test reading a Parquet file via
the s3() table function. The `Cannot read all data` assertion below is kept
as a guard but is not actually exercised by these queries.
"""

import io
import logging

import pytest

from helpers.cluster import ClickHouseCluster

logger = logging.getLogger(__name__)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["config.d/storage.xml"],
    with_minio=True,
    stay_alive=True,
)

BUCKET = "root"
S3_PREFIX = "data/"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _drop_cache(node):
    node.query("SYSTEM DROP FILESYSTEM CACHE")


def _get_data_objects(minio_client, prefix="data/"):
    """Return list of (object_name, size) tuples for all S3 data objects."""
    objects = list(minio_client.list_objects(BUCKET, prefix=prefix, recursive=True))
    return [(obj.object_name, obj.size) for obj in objects if obj.size > 0]


def _truncate_s3_object(minio_client, object_name, original_size, keep_fraction=0.5):
    """
    Replace an S3 object with shorter content (zeros), simulating
    the object being overwritten with a smaller version.
    """
    truncated_size = max(1, int(original_size * keep_fraction))
    truncated_data = b"\x00" * truncated_size
    buf = io.BytesIO(truncated_data)
    minio_client.put_object(BUCKET, object_name, buf, len(truncated_data))
    logger.info(
        "Truncated %s from %d to %d bytes", object_name, original_size, truncated_size
    )
    return truncated_size


def test_truncated_object_no_logical_error(started_cluster):
    """
    After truncating an S3 data object, a SELECT must NOT produce
    LOGICAL_ERROR about failed predownload or readBigAt.
    Any other error (checksum mismatch, broken part, etc.) is acceptable.
    """
    minio = started_cluster.minio_client
    node.query(
        """
        CREATE TABLE t_trunc (x UInt64, s String)
        ENGINE = MergeTree ORDER BY x
        SETTINGS storage_policy = 's3_cache',
                 min_bytes_for_wide_part = 0
        """
    )
    try:
        # Insert enough rows to produce non-trivial S3 objects
        node.query(
            "INSERT INTO t_trunc SELECT number, repeat(toString(number), 100) "
            "FROM numbers(10000)"
        )
        # Force data to be flushed to S3
        node.query("OPTIMIZE TABLE t_trunc FINAL")

        # Find the largest data object. With wide parts (min_bytes_for_wide_part = 0)
        # every column is a separate .bin object, and the largest one is the data
        # for the `s` String column — by far bigger than the UInt64 `x` column.
        objects = _get_data_objects(minio)
        assert len(objects) > 0, "No S3 objects found for the table"

        objects.sort(key=lambda x: x[1], reverse=True)
        target_name, target_size = objects[0]
        logger.info(
            "Target object for truncation: %s (%d bytes)", target_name, target_size
        )
        assert target_size > 100, f"Target object too small: {target_size}"

        # Drop filesystem cache so the next read must go through S3
        _drop_cache(node)

        # Truncate the S3 object to 50% of its original size
        _truncate_s3_object(minio, target_name, target_size, keep_fraction=0.5)

        # Read the data. The query MUST read the `s` column so it actually hits
        # the truncated object (the largest one) — reading only `x`/count() would
        # never touch it and the test would pass even on the unpatched code.
        # This should NOT throw LOGICAL_ERROR about predownload. Other errors are
        # acceptable (broken part, checksum mismatch, etc.).
        try:
            node.query("SELECT sum(cityHash64(s)), count() FROM t_trunc")
            logger.info("Query succeeded despite truncated object (bypass-cache worked)")
        except Exception as e:
            error_msg = str(e)
            # These are the specific LOGICAL_ERROR messages our fix prevents:
            assert "Failed to predownload remaining" not in error_msg, (
                f"Bug: LOGICAL_ERROR from predownloadForFileSegment: {error_msg}"
            )
            assert "Cannot read all data" not in error_msg, (
                f"Bug: LOGICAL_ERROR from readBigAt: {error_msg}"
            )
            assert "Having zero bytes, but range is not finished" not in error_msg, (
                f"Bug: LOGICAL_ERROR from readFromFileSegment: {error_msg}"
            )
            # Any other error is fine — checksum failure, broken part, etc.
            logger.info("Query raised non-LOGICAL_ERROR exception (expected): %s", error_msg[:200])
    finally:
        node.query("DROP TABLE IF EXISTS t_trunc SYNC")


def test_truncated_object_during_merge_read(started_cluster):
    """
    Truncating an S3 object during a merge-related read must not
    produce a LOGICAL_ERROR. The merge may fail, but the server must
    stay alive.
    """
    minio = started_cluster.minio_client
    node.query(
        """
        CREATE TABLE t_trunc_merge (x UInt64, s String)
        ENGINE = MergeTree ORDER BY x
        SETTINGS storage_policy = 's3_cache',
                 min_bytes_for_wide_part = 0
        """
    )
    try:
        # Insert multiple parts
        for i in range(3):
            node.query(
                f"INSERT INTO t_trunc_merge SELECT number + {i * 5000}, "
                f"repeat(toString(number + {i * 5000}), 50) "
                "FROM numbers(5000)"
            )

        # Find objects before merge
        objects_before = _get_data_objects(minio)
        assert len(objects_before) > 0

        # Drop cache
        _drop_cache(node)

        # Truncate the largest object (the `s` String column data).
        objects_before.sort(key=lambda x: x[1], reverse=True)
        target_name, target_size = objects_before[0]
        _truncate_s3_object(minio, target_name, target_size, keep_fraction=0.3)

        # Try reading the `s` column so the read actually touches the truncated
        # object — a bare count() reads only metadata and would never hit it.
        # Same assertion: no LOGICAL_ERROR from predownload.
        try:
            result = node.query("SELECT sum(cityHash64(s)) FROM t_trunc_merge")
            logger.info("Read succeeded: %s", result.strip())
        except Exception as e:
            error_msg = str(e)
            assert "Failed to predownload remaining" not in error_msg, (
                f"Bug: LOGICAL_ERROR from predownloadForFileSegment: {error_msg}"
            )
            assert "Cannot read all data" not in error_msg, (
                f"Bug: LOGICAL_ERROR from readBigAt: {error_msg}"
            )
            assert "Having zero bytes, but range is not finished" not in error_msg, (
                f"Bug: LOGICAL_ERROR from readFromFileSegment: {error_msg}"
            )
            logger.info("Query raised acceptable exception: %s", error_msg[:200])

        # Verify the server is still alive
        assert node.query("SELECT 1").strip() == "1", "Server should be alive after truncated read"
    finally:
        node.query("DROP TABLE IF EXISTS t_trunc_merge SYNC")
