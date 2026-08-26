import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_cache"
NUM_ROWS = 100000


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node",
        main_configs=["configs/storage_conf.xml"],
        with_rustfs=True,
        stay_alive=True,
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_cache_over_ca_startup_and_roundtrip():
    # Before the fix the server fails to register the cache-over-CA disk (NOT_IMPLEMENTED at
    # checkAccess), so this whole module fails at cluster.start(). After the fix, startup + a
    # write/read round-trip succeed.
    node = cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS cas_cache_test SYNC")
    node.query(
        """
        CREATE TABLE cas_cache_test (id Int64, data String)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy = '{}'
        """.format(
            STORAGE_POLICY
        )
    )
    node.query(
        "INSERT INTO cas_cache_test SELECT number, toString(number) FROM numbers({})".format(
            NUM_ROWS
        )
    )
    expected_sum = (NUM_ROWS - 1) * NUM_ROWS // 2
    assert int(node.query("SELECT count() FROM cas_cache_test")) == NUM_ROWS
    assert int(node.query("SELECT sum(id) FROM cas_cache_test")) == expected_sum

    node.query("DROP TABLE cas_cache_test SYNC")


def _profile_event(node, query_id, event):
    node.query("SYSTEM FLUSH LOGS")
    v = node.query(
        "SELECT sum(ProfileEvents['{}']) FROM system.query_log "
        "WHERE query_id = '{}' AND type = 'QueryFinish'".format(event, query_id)
    ).strip()
    return int(v) if v else 0


def test_cache_hits_on_repeated_reads():
    # The point of the feature: a second full scan of the same data is served from the local file
    # cache instead of re-fetching immutable content blobs from object storage.
    node = cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS cas_cache_metrics SYNC")
    node.query(
        """
        CREATE TABLE cas_cache_metrics (id Int64, data String)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy = '{}'
        """.format(
            STORAGE_POLICY
        )
    )
    node.query(
        "INSERT INTO cas_cache_metrics SELECT number, toString(number % 1000) FROM numbers(1000000)"
    )
    node.query("OPTIMIZE TABLE cas_cache_metrics FINAL")

    # Start from a cold cache.
    node.query("SYSTEM DROP FILESYSTEM CACHE")

    q1 = "cas_cache_cold_scan"
    node.query(
        "SELECT sum(cityHash64(id, data)) FROM cas_cache_metrics",
        query_id=q1,
        settings={"enable_filesystem_cache": 1},
    )
    # The cold read must POPULATE the cache (read-through), not just read from source: pin the write
    # side so a config where the cache never fills cannot pass on the warm-scan check alone.
    assert int(node.query("SELECT count() FROM system.filesystem_cache")) > 0

    q2 = "cas_cache_warm_scan"
    node.query(
        "SELECT sum(cityHash64(id, data)) FROM cas_cache_metrics",
        query_id=q2,
        settings={"enable_filesystem_cache": 1},
    )

    cold_source = _profile_event(node, q1, "CachedReadBufferReadFromSourceBytes")
    warm_source = _profile_event(node, q2, "CachedReadBufferReadFromSourceBytes")
    warm_cache = _profile_event(node, q2, "CachedReadBufferReadFromCacheBytes")

    assert cold_source > 0, "cold scan should read from source"
    assert warm_source * 10 < cold_source, (
        "warm scan should read far fewer source bytes (cold={}, warm={})".format(
            cold_source, warm_source
        )
    )
    assert warm_cache > 0, "warm scan should read from the filesystem cache"

    assert int(node.query("SELECT count() FROM system.filesystem_cache")) > 0

    node.query("DROP TABLE cas_cache_metrics SYNC")
