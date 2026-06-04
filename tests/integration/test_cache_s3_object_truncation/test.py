"""
Regression test for graceful handling of truncated S3 objects during cached reads.

When an S3 object is overwritten with shorter content between listing and
reading, the cache layer must NOT throw LOGICAL_ERROR and must NOT crash the
server. In predownloadForFileSegment, when the bytes the reader needs lie beyond
the truncated object there is no valid data to return, so the read fails with a
regular CANNOT_READ_ALL_DATA error -- not a LOGICAL_ERROR, and not a fabricated
EOF (a short/zero read there makes the caller consume uninitialized memory,
which aborts under MemorySanitizer and returns silent garbage in release).

Two conditions are required to actually exercise the predownload path, both
discovered empirically (a naive test passes on the buggy code too):

1. The object must be truncated to a VALID PREFIX of its real content, not
   overwritten with zeros. Zero-filled content fails decompression on the first
   block (UNKNOWN_CODEC) before the predownload path is reached.

2. The query must SEEK to a granule located beyond the truncation offset (a
   point query on the primary key), so the cache must predownload past the real
   EOF. A full-scan instead fails earlier in the compression layer
   (CANNOT_READ_ALL_DATA) and never reaches the predownload path.

NOTE on readBigAt: readBigAt is the random-access path used by the Parquet
reader over object storage, and is not reached by a MergeTree SELECT. It cannot
be exercised deterministically from a single-node query, because reading a
truncated object via the s3() table function re-fetches the object size (a fresh
HEAD returns the new, smaller size) and so never over-reads — it surfaces as a
plain INCORRECT_DATA "Not a Parquet file" error instead. The readBigAt guard in
the fix mirrors the readFromFileSegment EOF logic that this test does cover.
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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _get_data_objects(minio_client, prefix="data/"):
    """Return list of (object_name, size) tuples for all non-empty S3 data objects."""
    objects = list(minio_client.list_objects(BUCKET, prefix=prefix, recursive=True))
    return [(obj.object_name, obj.size) for obj in objects if obj.size > 0]


def _read_s3_object(minio_client, object_name):
    response = minio_client.get_object(BUCKET, object_name)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def _truncate_to_valid_prefix(minio_client, object_name, keep_fraction=0.5):
    """
    Replace an S3 object with a VALID PREFIX of its real content: the same
    leading bytes, just shorter. This simulates the object being overwritten
    with shorter content while the leading data is still readable — the
    condition that makes the cache predownload run past the real EOF.

    Replacing with zeros instead would fail decompression on the very first
    block (UNKNOWN_CODEC) before the predownload path is ever reached.
    """
    data = _read_s3_object(minio_client, object_name)
    truncated = data[: max(1, int(len(data) * keep_fraction))]
    minio_client.put_object(BUCKET, object_name, io.BytesIO(truncated), len(truncated))
    logger.info(
        "Truncated %s to a valid prefix: %d -> %d bytes",
        object_name,
        len(data),
        len(truncated),
    )
    return len(data), len(truncated)


# Substrings of the three LOGICAL_ERROR messages that the fix prevents. The
# readBigAt message is matched on its specific "Offset:" phrasing so it is not
# confused with the benign compression-layer "Cannot read all data. Bytes read:"
# (CANNOT_READ_ALL_DATA), which is an acceptable error for a truncated object.
FORBIDDEN_LOGICAL_ERRORS = [
    "Failed to predownload remaining",  # predownloadForFileSegment
    "Cannot read all data. Offset:",  # readBigAt
    "Having zero bytes, but range is not finished",  # readFromFileSegment
]


def _assert_no_forbidden_logical_error(error_msg):
    for needle in FORBIDDEN_LOGICAL_ERRORS:
        assert needle not in error_msg, (
            f"Bug: pre-fix LOGICAL_ERROR resurfaced ({needle!r}): {error_msg}"
        )


def test_truncated_object_predownload_no_logical_error(started_cluster):
    """
    Truncate the column data object to a valid prefix, then seek (via a point
    query on the primary key) to a granule located beyond the truncation. On the
    unpatched code this throws `Failed to predownload remaining ... bytes`
    (LOGICAL_ERROR); the fix fails with a regular CANNOT_READ_ALL_DATA and the
    server stays up. Any non-LOGICAL_ERROR (CANNOT_READ_ALL_DATA, broken part,
    etc.) is acceptable; what matters is no forbidden LOGICAL_ERROR and that the
    server survives (no use-of-uninitialized-value crash).
    """
    minio = started_cluster.minio_client
    node.query("DROP TABLE IF EXISTS t_trunc SYNC")
    node.query(
        """
        CREATE TABLE t_trunc (x UInt64, s String)
        ENGINE = MergeTree ORDER BY x
        SETTINGS storage_policy = 's3_cache',
                 min_bytes_for_wide_part = 0,
                 index_granularity = 8192
        """
    )
    try:
        # A single INSERT well under max_insert_block_size produces one part, so
        # the largest object is unambiguously that part's `s` column data.
        node.query(
            "INSERT INTO t_trunc SELECT number, repeat(toString(number), 100) "
            "FROM numbers(100000)"
        )
        active_parts = node.query(
            "SELECT count() FROM system.parts WHERE table = 't_trunc' AND active"
        ).strip()
        assert active_parts == "1", f"expected a single part, got {active_parts}"

        objects = _get_data_objects(minio)
        objects.sort(key=lambda x: x[1], reverse=True)
        target_name, target_size = objects[0]
        logger.info("Truncating largest object %s (%d bytes)", target_name, target_size)
        # Must be large enough that a late granule sits beyond the 50% cut.
        assert target_size > 100000, f"target object too small: {target_size}"

        node.query("SYSTEM DROP FILESYSTEM CACHE")
        _truncate_to_valid_prefix(minio, target_name, keep_fraction=0.5)

        # Point query on the PK seeks to a granule near the end of the column,
        # forcing the cache to predownload past the real (truncated) EOF.
        try:
            node.query(
                "SELECT s FROM t_trunc WHERE x = 99000 "
                "SETTINGS enable_filesystem_cache = 1, max_threads = 1"
            )
            logger.info("Query returned without error (truncation handled as EOF)")
        except Exception as e:
            _assert_no_forbidden_logical_error(str(e))
            logger.info("Query raised acceptable non-LOGICAL_ERROR: %s", str(e)[:200])

        # Server must stay alive.
        assert node.query("SELECT 1").strip() == "1"
    finally:
        node.query("DROP TABLE IF EXISTS t_trunc SYNC")
