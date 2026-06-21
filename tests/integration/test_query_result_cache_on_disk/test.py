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


def test_disk_cache_respects_memory_quota_after_restart(start_cluster):
    setup_table()

    # `default` writes an entry to the disk cache with no per-user quota.
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
    assert node.query("SELECT count() FROM system.query_cache WHERE type = 'Disk'").strip() == "1"

    # After a restart only the disk metadata is reloaded; the per-user memory quota map is empty
    # (`PerUserTTLCachePolicyUserQuota` then treats the user as unlimited).
    node.restart_clickhouse(kill=True)
    assert node.query("SELECT count() FROM system.query_cache WHERE type = 'Memory'").strip() == "0"

    # Reading the restored entry with a tiny per-user memory quota (`query_cache_max_size_in_bytes = 1`) must
    # still serve it from disk, but the disk->memory promotion must be declined by the quota - otherwise reading
    # from disk would push `QueryCacheBytes` past the profile cap.
    query_id = "qrc_on_disk_quota_read"
    res = node.query(
        QUERY,
        query_id=query_id,
        settings={
            "use_query_cache": 1,
            "enable_reads_from_query_cache_disk": 1,
            "query_cache_max_size_in_bytes": 1,
        },
    )
    assert res == expected
    assert disk_hits_for(query_id) == 1
    # The promotion was rejected by the quota, so the memory cache stays empty.
    assert node.query("SELECT count() FROM system.query_cache WHERE type = 'Memory'").strip() == "0"


def test_disk_cache_user_isolation_after_restart(start_cluster):
    setup_table()
    # `other` is granted `SELECT ON default.t` via `configs/users.xml` (it lives in the readonly
    # `users.xml` storage and therefore cannot be granted via SQL).

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


CACHE_ROOT = "/var/lib/clickhouse/query_result_cache"


def test_disk_load_does_not_delete_foreign_dirs(start_cluster):
    setup_table()

    # Write a real entry so the on-disk cache root exists and is non-empty.
    node.query(
        QUERY,
        settings={
            "use_query_cache": 1,
            "enable_writes_to_query_cache_disk": 1,
            "enable_reads_from_query_cache_disk": 1,
            "query_cache_ttl": 600,
        },
    )
    assert (
        node.query(
            "SELECT count() FROM system.query_cache WHERE type = 'Disk'"
        ).strip()
        == "1"
    )
    assert (
        node.exec_in_container(
            ["bash", "-c", f"test -d '{CACHE_ROOT}' && echo yes"], user="root"
        ).strip()
        == "yes"
    )

    # Plant foreign content under the cache path:
    #  - a foreign first-level directory whose name is not a hash bucket (1 to 3 digits), and
    #  - a foreign entry directory inside a bucket-shaped directory whose name does not match the
    #    "<low64>_<high64>" cache-entry pattern.
    # `loadEntrysFromDisk` must skip both (they are not query-result-cache entries) and must never remove
    # them. This is the fail-closed guard against a misconfigured `query_cache.path` pointing at a directory
    # that also holds unrelated data.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p '{CACHE_ROOT}/foreign_dir/sub' && echo keep > '{CACHE_ROOT}/foreign_dir/sub/important.txt'",
        ],
        user="root",
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p '{CACHE_ROOT}/999/foreign_entry' && echo keep > '{CACHE_ROOT}/999/foreign_entry/important.txt'",
        ],
        user="root",
    )

    node.restart_clickhouse(kill=True)

    # The real cache entry is still loaded ...
    assert (
        node.query(
            "SELECT count() FROM system.query_cache WHERE type = 'Disk'"
        ).strip()
        == "1"
    )
    # ... and the foreign content was preserved, not deleted as a "broken" cache entry.
    assert (
        node.exec_in_container(
            ["bash", "-c", f"cat '{CACHE_ROOT}/foreign_dir/sub/important.txt'"],
            user="root",
        ).strip()
        == "keep"
    )
    assert (
        node.exec_in_container(
            ["bash", "-c", f"cat '{CACHE_ROOT}/999/foreign_entry/important.txt'"],
            user="root",
        ).strip()
        == "keep"
    )


def test_disk_cache_shared_entry_quota_after_restart(start_cluster):
    setup_table()

    # `default` writes a SHARED entry (`query_cache_share_between_users = 1`) to the disk cache.
    expected = node.query(
        QUERY,
        settings={
            "use_query_cache": 1,
            "query_cache_share_between_users": 1,
            "enable_writes_to_query_cache_disk": 1,
            "enable_reads_from_query_cache_disk": 1,
            "query_cache_ttl": 600,
        },
    )
    assert (
        node.query(
            "SELECT shared FROM system.query_cache WHERE type = 'Disk'"
        ).strip()
        == "1"
    )

    node.restart_clickhouse(kill=True)
    assert (
        node.query(
            "SELECT count() FROM system.query_cache WHERE type = 'Memory'"
        ).strip()
        == "0"
    )

    # A different user (`other`) reads the shared entry from disk. The disk->memory promotion accounts the
    # entry under the *writer*'s (`default`'s) `user_id`; doing so with the *reader*'s quota would let `other`
    # overwrite `default`'s memory-cache quota and promote `default`-owned entries past `default`'s limit.
    # For a cross-user shared read the entry is still served, but it must NOT be promoted into memory.
    query_id = "qrc_on_disk_shared_other_read"
    res = node.query(
        QUERY,
        user="other",
        query_id=query_id,
        settings={
            "use_query_cache": 1,
            "query_cache_share_between_users": 1,
            "enable_reads_from_query_cache_disk": 1,
            "query_cache_max_size_in_bytes": 1,
        },
    )
    assert res == expected
    assert disk_hits_for(query_id) == 1
    # Cross-user shared read does not promote into memory, so the owner's quota state is left untouched.
    assert (
        node.query(
            "SELECT count() FROM system.query_cache WHERE type = 'Memory'"
        ).strip()
        == "0"
    )

    # The owner (`default`) reading its own shared entry DOES promote it into memory (reader == owner).
    query_id = "qrc_on_disk_shared_owner_read"
    res = node.query(
        QUERY,
        query_id=query_id,
        settings={
            "use_query_cache": 1,
            "query_cache_share_between_users": 1,
            "enable_reads_from_query_cache_disk": 1,
        },
    )
    assert res == expected
    assert disk_hits_for(query_id) == 1
    assert (
        node.query(
            "SELECT count() FROM system.query_cache WHERE type = 'Memory'"
        ).strip()
        == "1"
    )


def test_disk_cache_tag_with_special_chars_survives_restart(start_cluster):
    setup_table()

    # `query_cache_tag` is documented as accepting any string. A tag that contains a newline and a tab is
    # valid input and must round-trip through the on-disk `key_metadata.txt` (the string fields are written
    # with an escaped encoding). Otherwise `Key::deserialize` would truncate the tag at the first '\n'/'\t',
    # `loadEntrysFromDisk` would classify the entry as broken, and it would be silently discarded after a
    # restart - so the cache would lose entries for any query run with such a tag.
    tag = "weird\ttag\nwith\\specials"
    expected = node.query(
        QUERY,
        settings={
            "use_query_cache": 1,
            "enable_writes_to_query_cache_disk": 1,
            "enable_reads_from_query_cache_disk": 1,
            "query_cache_tag": tag,
            # Keep the entry fresh across the (potentially slow) restart so the post-restart read is a hit.
            "query_cache_ttl": 600,
        },
    )
    assert node.query("SELECT count() FROM system.query_cache WHERE type = 'Disk'").strip() == "1"
    # `hex()` avoids TSV-escaping ambiguity when comparing a tag that itself contains tabs and newlines.
    tag_hex = node.query("SELECT hex(tag) FROM system.query_cache WHERE type = 'Disk'").strip()
    assert tag_hex != ""

    node.restart_clickhouse(kill=True)

    # The entry must still be present after restart (not discarded as broken) and the tag must be byte-identical.
    assert node.query("SELECT count() FROM system.query_cache WHERE type = 'Disk'").strip() == "1"
    assert node.query("SELECT hex(tag) FROM system.query_cache WHERE type = 'Disk'").strip() == tag_hex

    # And the restored entry is fully usable: a read with the same tag is served from disk and matches.
    query_id = "qrc_on_disk_special_tag_read"
    res = node.query(
        QUERY,
        query_id=query_id,
        settings={
            "use_query_cache": 1,
            "enable_reads_from_query_cache_disk": 1,
            "query_cache_tag": tag,
        },
    )
    assert res == expected
    assert disk_hits_for(query_id) == 1
