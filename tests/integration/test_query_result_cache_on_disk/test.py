import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

# A second node whose `query_cache.path` is deliberately misconfigured to an unsafe value (an absolute path,
# then a parent-traversal path). It must start with the on-disk cache disabled instead of touching data outside
# the configured cache directory.
node_bad_path = cluster.add_instance(
    "node_bad_path",
    main_configs=["configs/unsafe_query_cache_path.xml"],
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
    #  - a foreign first-level directory whose name is not a hash bucket (1 to 3 digits),
    #  - a foreign entry directory inside a bucket-shaped directory whose name does not match the
    #    "<low64>_<high64>" cache-entry pattern, and
    #  - a foreign directory whose name DOES match the cache-entry pattern ("999/123_456") but which has no
    #    `key_metadata.txt`, i.e. it is not actually a query-result-cache entry.
    # `loadEntrysFromDisk` must skip all three (they are not query-result-cache entries) and must never remove
    # them. This is the fail-closed guard against a misconfigured `query_cache.path` pointing at a directory
    # that also holds unrelated data. The third case is the important one: a name match alone is not proof of
    # ownership, so the loader must additionally require the `key_metadata.txt` ownership marker before treating
    # a directory as a (possibly broken) cache entry to be removed.
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
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p '{CACHE_ROOT}/999/123_456' && echo keep > '{CACHE_ROOT}/999/123_456/important.txt'",
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
    # The cache-shaped but metadata-less directory must be preserved too (no `key_metadata.txt` => not ours).
    assert (
        node.exec_in_container(
            ["bash", "-c", f"cat '{CACHE_ROOT}/999/123_456/important.txt'"],
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


def _assert_unsafe_path_disables_disk_cache(cache_root):
    # `cache_root` is where the (unsafe) `query_cache.path` resolves on disk. Plant a cache-shaped directory
    # (`<bucket>/<low64>_<high64>`, see `Key::getKeyPath`) that holds unrelated data. Without the fail-closed
    # guard, `loadEntrysFromDisk` would walk this path, fail to parse the planted directory as a cache entry,
    # and `removeRecursive` it as "broken" - deleting data outside the configured cache directory.
    node_bad_path.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p '{cache_root}/999/123_456' && echo keep > '{cache_root}/999/123_456/important.txt'",
        ],
        user="root",
    )

    node_bad_path.restart_clickhouse(kill=True)

    # The on-disk cache must be disabled, so a write with `enable_writes_to_query_cache_disk = 1` produces no
    # disk entry (the memory cache still works) and the server logs that the cache was disabled.
    node_bad_path.query("SYSTEM DROP QUERY CACHE")
    node_bad_path.query("DROP TABLE IF EXISTS t SYNC")
    node_bad_path.query("CREATE TABLE t (id Int64, c String) ENGINE = MergeTree ORDER BY id")
    node_bad_path.query("INSERT INTO t SELECT number, concat('abc_', number) FROM numbers(10)")
    node_bad_path.query(
        QUERY,
        settings={
            "use_query_cache": 1,
            "enable_writes_to_query_cache_disk": 1,
            "enable_reads_from_query_cache_disk": 1,
        },
    )
    assert (
        node_bad_path.query(
            "SELECT count() FROM system.query_cache WHERE type = 'Disk'"
        ).strip()
        == "0"
    )
    assert node_bad_path.contains_in_log("on-disk query result cache is disabled")

    # The planted data outside the configured cache directory must be untouched (never walked or removed).
    assert (
        node_bad_path.exec_in_container(
            ["bash", "-c", f"cat '{cache_root}/999/123_456/important.txt'"], user="root"
        ).strip()
        == "keep"
    )


def test_unsafe_disk_path_disables_cache_and_preserves_data(start_cluster):
    # An absolute `query_cache.path` replaces the disk root entirely on `DiskLocal` (`fs::path(root) / path`),
    # so it must be rejected and the on-disk cache disabled.
    _assert_unsafe_path_disables_disk_cache("/var/lib/protected_qrc_abs")

    # A parent-traversal `query_cache.path` (`../protected_qrc_rel`) escapes the disk root (`/var/lib/clickhouse`),
    # resolving to `/var/lib/protected_qrc_rel`; it must be rejected too.
    node_bad_path.replace_in_config(
        "/etc/clickhouse-server/config.d/unsafe_query_cache_path.xml",
        "/var/lib/protected_qrc_abs",
        "../protected_qrc_rel",
    )
    _assert_unsafe_path_disables_disk_cache("/var/lib/protected_qrc_rel")

    # An *in-root* `..` (`qrc_link/../escaped_qrc`) lexically normalizes to `escaped_qrc` and would pass a
    # normalized-only safety check, but on disk `/var/lib/clickhouse/qrc_link/../escaped_qrc` is resolved by the
    # kernel relative to the *target* of `qrc_link` if it is a symlink, escaping the disk root. The path must be
    # rejected because it contains a `..` component at all, regardless of whether `qrc_link` is a symlink.
    node_bad_path.replace_in_config(
        "/etc/clickhouse-server/config.d/unsafe_query_cache_path.xml",
        "../protected_qrc_rel",
        "qrc_link/../escaped_qrc",
    )
    # `escaped_qrc` is where the path lexically normalizes (and where a normalized-only check would wrongly enable
    # the cache); assert no disk entry is created there and the cache is reported disabled.
    _assert_unsafe_path_disables_disk_cache("/var/lib/clickhouse/escaped_qrc")
