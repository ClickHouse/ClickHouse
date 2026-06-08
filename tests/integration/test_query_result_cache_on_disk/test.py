import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

QUERY = "SELECT id, c FROM t ORDER BY id"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def setup_table():
    node.query("SYSTEM DROP QUERY CACHE")
    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query("CREATE TABLE t (id Int64, c String) ENGINE = MergeTree ORDER BY id")
    node.query("INSERT INTO t SELECT number, concat('abc_', number) FROM numbers(10)")


def disk_hits_for(query_id):
    node.query("SYSTEM FLUSH LOGS query_log")
    return int(
        node.query(
            f"""
            SELECT sum(ProfileEvents['QueryCacheDiskHits'])
            FROM system.query_log
            WHERE query_id = '{query_id}' AND type = 'QueryFinish'
            """
        ).strip()
    )


def test_disk_cache_survives_restart(start_cluster):
    setup_table()

    expected = node.query(
        QUERY,
        settings={
            "use_query_cache": 1,
            "enable_writes_to_query_cache_disk": 1,
            "enable_reads_from_query_cache_disk": 1,
            # Keep the entry fresh across the (potentially slow) restart so the post-restart read is a hit.
            "query_cache_ttl": 600,
        },
    )

    # The entry was written to both the memory and the disk cache.
    assert node.query("SELECT count() FROM system.query_cache WHERE type = 'Disk'").strip() == "1"

    # A restart wipes the in-memory cache; the disk entry must be reloaded from its on-disk metadata
    # (`Key::deserialize` / `loadDiskEntryHeaderAndSize`), i.e. the persistence boundary of the feature.
    node.restart_clickhouse(kill=True)

    assert node.query("SELECT count() FROM system.query_cache WHERE type = 'Memory'").strip() == "0"
    assert node.query("SELECT count() FROM system.query_cache WHERE type = 'Disk'").strip() == "1"

    # With disk reads enabled, the restored entry is served (a disk hit) and repopulates the memory cache.
    query_id = "qrc_on_disk_restart_read"
    res = node.query(
        QUERY,
        query_id=query_id,
        settings={"use_query_cache": 1, "enable_reads_from_query_cache_disk": 1},
    )
    assert res == expected
    assert disk_hits_for(query_id) == 1
    assert node.query("SELECT count() FROM system.query_cache WHERE type = 'Memory'").strip() == "1"

    # With disk reads disabled, the restored entry must not be served from disk.
    node.restart_clickhouse(kill=True)
    query_id = "qrc_on_disk_restart_read_disabled"
    node.query(
        QUERY,
        query_id=query_id,
        settings={"use_query_cache": 1, "enable_reads_from_query_cache_disk": 0},
    )
    assert disk_hits_for(query_id) == 0


def test_disk_cache_user_isolation_after_restart(start_cluster):
    setup_table()
    node.query("GRANT SELECT ON default.t TO other")

    # `default` writes a per-user (non-shared) entry to the disk cache.
    node.query(
        QUERY,
        settings={
            "use_query_cache": 1,
            "enable_writes_to_query_cache_disk": 1,
            "enable_reads_from_query_cache_disk": 1,
            # Keep the entry fresh across the (potentially slow) restart so the post-restart read is a hit.
            "query_cache_ttl": 600,
        },
    )
    assert node.query("SELECT shared FROM system.query_cache WHERE type = 'Disk'").strip() == "0"

    node.restart_clickhouse(kill=True)

    # `default` can read its own restored entry from disk ...
    query_id = "qrc_on_disk_owner_read"
    node.query(
        QUERY,
        query_id=query_id,
        settings={"use_query_cache": 1, "enable_reads_from_query_cache_disk": 1},
    )
    assert disk_hits_for(query_id) == 1

    # ... but `other` must not: the restored access metadata keeps the entry user-scoped.
    query_id = "qrc_on_disk_other_read"
    node.query(
        QUERY,
        user="other",
        query_id=query_id,
        settings={"use_query_cache": 1, "enable_reads_from_query_cache_disk": 1},
    )
    assert disk_hits_for(query_id) == 0
