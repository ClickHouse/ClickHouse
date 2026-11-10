import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["config.d/storage.xml"],
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _drop_cache(node):
    """Drop all filesystem cache to ensure next read goes via S3 → cache write path."""
    node.query("SYSTEM DROP FILESYSTEM CACHE")


def test_skip_cache_on_disk_failure_select(started_cluster):
    """skip=true: failpoint triggers during SELECT, query should succeed (fall back to S3), no broken parts."""
    node.query(
        """
        CREATE TABLE t_skip_sel (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS storage_policy = 's3_cache_skip'
        """
    )
    try:
        node.query("INSERT INTO t_skip_sel SELECT number FROM numbers(100)")

        # Drop cache so next read goes via S3 → writeCache() path, triggering the failpoint
        _drop_cache(node)
        node.query("SYSTEM ENABLE FAILPOINT cache_filesystem_failure")
        try:
            # Use sum() to force real column data read, bypassing optimize_trivial_count_query
            result = node.query("SELECT sum(x) FROM t_skip_sel")
            assert result.strip() == str(sum(range(100))), f"Unexpected result: {result}"

            # No parts should be moved to detached
            broken = node.query(
                "SELECT count() FROM system.detached_parts "
                "WHERE table = 't_skip_sel' AND database = currentDatabase()"
            )
            assert broken.strip() == "0", f"Unexpected detached parts: {broken}"
        finally:
            node.query("SYSTEM DISABLE FAILPOINT cache_filesystem_failure")
    finally:
        node.query("DROP TABLE IF EXISTS t_skip_sel")


def test_noskip_cache_on_disk_failure_select(started_cluster):
    """skip=false (default): failpoint triggers during SELECT, should raise CACHE_CANNOT_WRITE_TO_CACHE_DISK."""
    node.query(
        """
        CREATE TABLE t_noskip_sel (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS storage_policy = 's3_cache_noskip'
        """
    )
    try:
        node.query("INSERT INTO t_noskip_sel SELECT number FROM numbers(100)")

        # Drop cache so next read goes via S3 → writeCache() path, triggering the failpoint
        _drop_cache(node)
        node.query("SYSTEM ENABLE FAILPOINT cache_filesystem_failure")
        try:
            with pytest.raises(Exception) as exc_info:
                node.query("SELECT sum(x) FROM t_noskip_sel")
            assert "CACHE_CANNOT_WRITE_TO_CACHE_DISK" in str(exc_info.value)
        finally:
            node.query("SYSTEM DISABLE FAILPOINT cache_filesystem_failure")

        # After disabling the failpoint, SELECT should succeed again
        result = node.query("SELECT sum(x) FROM t_noskip_sel")
        assert result.strip() == str(sum(range(100)))
    finally:
        node.query("DROP TABLE IF EXISTS t_noskip_sel")


def test_skip_cache_on_disk_failure_attach(started_cluster):
    """
    skip=true: failpoint active during ATTACH TABLE (loadDataPart path).
    Part loading should succeed (bypass cache write, read from S3 directly).
    system.detached_parts must remain empty.
    """
    node.query(
        """
        CREATE TABLE t_skip_att (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS storage_policy = 's3_cache_skip'
        """
    )
    try:
        node.query("INSERT INTO t_skip_att SELECT number FROM numbers(100)")
        node.query("DETACH TABLE t_skip_att")
        _drop_cache(node)

        node.query("SYSTEM ENABLE FAILPOINT cache_filesystem_failure")
        try:
            node.query("ATTACH TABLE t_skip_att")
            node.query("SYSTEM WAIT LOADING PARTS t_skip_att")

            result = node.query("SELECT sum(x) FROM t_skip_att")
            assert result.strip() == str(sum(range(100)))

            broken = node.query(
                "SELECT count() FROM system.detached_parts "
                "WHERE table = 't_skip_att' AND database = currentDatabase()"
            )
            assert broken.strip() == "0", f"Unexpected detached parts: {broken}"
        finally:
            node.query("SYSTEM DISABLE FAILPOINT cache_filesystem_failure")
    finally:
        node.query("DROP TABLE IF EXISTS t_skip_att")


def test_noskip_cache_on_disk_failure_attach(started_cluster):
    """
    skip=false (default): failpoint active during ATTACH TABLE (loadDataPart path).
    - ATTACH should fail with CACHE_CANNOT_WRITE_TO_CACHE_DISK.
    - Parts must NOT be moved to detached (core assertion: not marked broken).
    - After disabling the failpoint, ATTACH should succeed.
    """
    node.query(
        """
        CREATE TABLE t_noskip_att (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS storage_policy = 's3_cache_noskip'
        """
    )
    try:
        node.query("INSERT INTO t_noskip_att SELECT number FROM numbers(100)")
        node.query("DETACH TABLE t_noskip_att")
        _drop_cache(node)

        node.query("SYSTEM ENABLE FAILPOINT cache_filesystem_failure")
        try:
            with pytest.raises(Exception) as exc_info:
                node.query("ATTACH TABLE t_noskip_att")
            assert "CACHE_CANNOT_WRITE_TO_CACHE_DISK" in str(exc_info.value)

            # Core assertion: parts must not be marked broken (moved to detached)
            broken = node.query(
                "SELECT count() FROM system.detached_parts "
                "WHERE table = 't_noskip_att' AND database = currentDatabase()"
            )
            assert broken.strip() == "0", (
                f"Bug: parts moved to detached despite cache-only failure: {broken}"
            )
        finally:
            node.query("SYSTEM DISABLE FAILPOINT cache_filesystem_failure")

        # After disabling the failpoint, ATTACH should succeed
        node.query("ATTACH TABLE t_noskip_att")
        node.query("SYSTEM WAIT LOADING PARTS t_noskip_att")
        result = node.query("SELECT sum(x) FROM t_noskip_att")
        assert result.strip() == str(sum(range(100)))
    finally:
        node.query("DROP TABLE IF EXISTS t_noskip_att")
