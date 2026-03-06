import json
import time
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
)


ICEBERG_SETTINGS = {"allow_insert_into_iceberg": 1}
FAR_FUTURE = "2099-12-31 23:59:59"
AGGRESSIVE_RETENTION = {
    "history.expire.max-snapshot-age-ms": "1",
    "history.expire.min-snapshots-to-keep": "1",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_iceberg_metadata(instance, table_name):
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    latest = instance.exec_in_container(
        ["bash", "-c", f"ls -t {metadata_dir}/*.metadata.json | head -1"]
    ).strip()
    raw = instance.exec_in_container(["cat", latest])
    return json.loads(raw), latest


def _write_iceberg_metadata(instance, table_name, meta, prev_path):
    import re

    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    meta["last-updated-ms"] = int(time.time() * 1000)
    version_match = re.search(r"/v(\d+)[^/]*\.metadata\.json$", prev_path)
    new_version = int(version_match.group(1)) + 1
    new_path = f"{metadata_dir}/v{new_version}.metadata.json"
    new_content = json.dumps(meta, indent=4)
    instance.exec_in_container(
        ["bash", "-c", f"cat > {new_path} << 'JSONEOF'\n{new_content}\nJSONEOF"]
    )


def read_iceberg_metadata(instance, table_name):
    meta, _ = _read_iceberg_metadata(instance, table_name)
    return meta


def update_iceberg_metadata(instance, table_name, updater_fn):
    meta, prev_path = _read_iceberg_metadata(instance, table_name)
    updater_fn(meta)
    _write_iceberg_metadata(instance, table_name, meta, prev_path)


def set_iceberg_table_properties(instance, table_name, properties):
    def updater(meta):
        meta.setdefault("properties", {}).update(properties)
    update_iceberg_metadata(instance, table_name, updater)


def create_and_populate(cluster, instance, storage_type, table_name, n_rows, format_version=2):
    create_iceberg_table(
        storage_type, instance, table_name, cluster, "(x Int)", format_version
    )
    for val in range(1, n_rows + 1):
        instance.query(
            f"INSERT INTO {table_name} VALUES ({val});",
            settings=ICEBERG_SETTINGS,
        )


def expire_snapshots(instance, table_name, timestamp=None):
    if timestamp:
        instance.query(
            f"ALTER TABLE {table_name} EXECUTE expire_snapshots('{timestamp}');",
            settings=ICEBERG_SETTINGS,
        )
    else:
        instance.query(
            f"ALTER TABLE {table_name} EXECUTE expire_snapshots();",
            settings=ICEBERG_SETTINGS,
        )


def get_snapshot_ids(instance, table_name):
    """Return snapshot IDs sorted by (timestamp, id) for stable ordering."""
    meta = read_iceberg_metadata(instance, table_name)
    pairs = [(s["snapshot-id"], s["timestamp-ms"]) for s in meta["snapshots"]]
    pairs.sort(key=lambda x: (x[1], x[0]))
    return [sid for sid, _ in pairs]


def get_retained_ids(instance, table_name):
    meta = read_iceberg_metadata(instance, table_name)
    return {s["snapshot-id"] for s in meta["snapshots"]}


def get_refs(instance, table_name):
    meta = read_iceberg_metadata(instance, table_name)
    return meta.get("refs", {})


def get_history_count(instance, table_name):
    return int(instance.query(
        f"SELECT count() FROM system.iceberg_history "
        f"WHERE database = 'default' AND table = '{table_name}'"
    ).strip())


def assert_data_intact(instance, table_name, n_rows):
    expected = "".join(f"{i}\n" for i in range(1, n_rows + 1))
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY x") == expected


def make_table_name(prefix, storage_type):
    return f"{prefix}_{storage_type}_{get_uuid_str()}"


# ---------------------------------------------------------------------------
# Basic / sanity tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_expire_snapshots_basic(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_basic", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )

    assert instance.query(f"SELECT count() FROM {TABLE_NAME}") == "3\n"
    assert get_history_count(instance, TABLE_NAME) >= 3

    time.sleep(2)
    expire_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (4);", settings=ICEBERG_SETTINGS
    )

    expire_snapshots(instance, TABLE_NAME, expire_timestamp)
    assert_data_intact(instance, TABLE_NAME, 4)


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_no_expirable(started_cluster_iceberg_with_spark, storage_type):
    """No-op when no snapshots match the criteria."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_no_expirable", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1
    )
    expire_snapshots(instance, TABLE_NAME, "2020-01-01 00:00:00")
    assert_data_intact(instance, TABLE_NAME, 1)


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_format_v1_error(started_cluster_iceberg_with_spark, storage_type):
    """expire_snapshots rejects format version 1 with BAD_ARGUMENTS."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_v1_error", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1,
        format_version=1,
    )

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots('2020-01-01 00:00:00');",
        settings=ICEBERG_SETTINGS,
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS error for v1, got: {error}"
    assert "second version" in error, f"Expected v2-only message, got: {error}"


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_preserves_current(started_cluster_iceberg_with_spark, storage_type):
    """Current snapshot is never expired, even with a far-future timestamp."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_preserves_current", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 2
    )
    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)
    assert_data_intact(instance, TABLE_NAME, 2)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_files_cleaned(started_cluster_iceberg_with_spark, storage_type):
    """Expired snapshot files are physically removed; retained snapshot files survive."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_files_cleaned", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 2
    )

    time.sleep(2)
    expire_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (3);", settings=ICEBERG_SETTINGS
    )

    set_iceberg_table_properties(instance, TABLE_NAME, AGGRESSIVE_RETENTION)

    meta_before = read_iceberg_metadata(instance, TABLE_NAME)
    current_id = meta_before["current-snapshot-id"]
    expired_manifest_lists = []
    retained_manifest_list = None
    for snap in meta_before["snapshots"]:
        ml = snap.get("manifest-list", "")
        if snap["snapshot-id"] == current_id:
            retained_manifest_list = ml
        else:
            expired_manifest_lists.append(ml)

    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"
    files_before = default_download_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    )

    expire_snapshots(instance, TABLE_NAME, expire_timestamp)

    files_after = default_download_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    )
    files_after_set = set(files_after)

    assert len(files_after) < len(files_before), \
        f"Expected fewer files after expire: {len(files_after)} >= {len(files_before)}"

    for ml in expired_manifest_lists:
        assert not any(ml in f for f in files_after_set), \
            f"Expired manifest-list should be deleted: {ml}"

    if retained_manifest_list:
        assert any(retained_manifest_list in f for f in files_after_set), \
            f"Current snapshot's manifest-list should survive: {retained_manifest_list}"

    assert_data_intact(instance, TABLE_NAME, 3)


# ---------------------------------------------------------------------------
# Retention policy tests (table-level properties)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_retention_min_keep(started_cluster_iceberg_with_spark, storage_type):
    """min-snapshots-to-keep prevents expiring the N most recent ancestors."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_min_keep", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 5
    )
    assert get_history_count(instance, TABLE_NAME) == 5

    set_iceberg_table_properties(instance, TABLE_NAME, {
        "history.expire.min-snapshots-to-keep": "3",
        "history.expire.max-snapshot-age-ms": "1",
    })
    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    assert get_history_count(instance, TABLE_NAME) == 3
    assert_data_intact(instance, TABLE_NAME, 5)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_retention_max_age(started_cluster_iceberg_with_spark, storage_type):
    """max-snapshot-age-ms preserves recent snapshots regardless of timestamp cutoff."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_max_age", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 5
    )

    set_iceberg_table_properties(instance, TABLE_NAME, {
        "history.expire.min-snapshots-to-keep": "1",
        "history.expire.max-snapshot-age-ms": str(3600 * 1000),
    })
    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    assert get_history_count(instance, TABLE_NAME) == 5, \
        "All 5 snapshots are within 1 hour, all should be retained"
    assert_data_intact(instance, TABLE_NAME, 5)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_no_args_default_retention(started_cluster_iceberg_with_spark, storage_type):
    """No-arg expire uses default 5-day max-age; all recent snapshots survive."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_no_args_default", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )
    assert get_history_count(instance, TABLE_NAME) == 3

    expire_snapshots(instance, TABLE_NAME)

    assert get_history_count(instance, TABLE_NAME) == 3, \
        "All snapshots < 5 days old, none should be expired"
    assert_data_intact(instance, TABLE_NAME, 3)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_no_args_with_short_max_age(started_cluster_iceberg_with_spark, storage_type):
    """No-arg expire with very short max-age expires old snapshots."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_no_args_short_age", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 5
    )
    time.sleep(2)

    set_iceberg_table_properties(instance, TABLE_NAME, AGGRESSIVE_RETENTION)
    expire_snapshots(instance, TABLE_NAME)

    assert get_history_count(instance, TABLE_NAME) == 1, \
        "With 1ms max-age and min-keep=1, only current snapshot should remain"
    assert_data_intact(instance, TABLE_NAME, 5)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_boundary_max_age(started_cluster_iceberg_with_spark, storage_type):
    """Two-phase boundary: generous max-age retains all, then 1ms max-age expires old."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_boundary_age", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )

    meta = read_iceberg_metadata(instance, TABLE_NAME)
    snap_ts = {s["snapshot-id"]: s["timestamp-ms"] for s in meta["snapshots"]}
    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    oldest_id = snap_ids[0]

    now_ms = int(time.time() * 1000)
    generous_age = (now_ms - snap_ts[oldest_id]) + 10000

    set_iceberg_table_properties(instance, TABLE_NAME, {
        "history.expire.max-snapshot-age-ms": str(generous_age),
        "history.expire.min-snapshots-to-keep": "1",
    })
    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    retained = get_retained_ids(instance, TABLE_NAME)
    assert oldest_id in retained, \
        f"Snapshot within max-age should be retained, but {oldest_id} expired"
    assert len(retained) == 3, f"All 3 within max-age, got {len(retained)}"

    set_iceberg_table_properties(instance, TABLE_NAME, AGGRESSIVE_RETENTION)
    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    retained = get_retained_ids(instance, TABLE_NAME)
    assert oldest_id not in retained, "Snapshot outside 1ms max-age should be expired"
    assert len(retained) == 1, f"Only current should remain, got {len(retained)}"
    assert_data_intact(instance, TABLE_NAME, 3)


# ---------------------------------------------------------------------------
# Ref behavior tests (branches, tags, dangling refs)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_tag_retained(started_cluster_iceberg_with_spark, storage_type):
    """A tag's snapshot is retained even when age/min-keep would otherwise expire it."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_tag_retained", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 5
    )

    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    current_id = snap_ids[-1]
    tag_id = snap_ids[1]

    def add_tag(meta):
        meta["refs"] = {
            "main": {"snapshot-id": current_id, "type": "branch"},
            "release-v1": {"snapshot-id": tag_id, "type": "tag"},
        }
        meta.setdefault("properties", {}).update(AGGRESSIVE_RETENTION)
    update_iceberg_metadata(instance, TABLE_NAME, add_tag)

    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    assert get_retained_ids(instance, TABLE_NAME) == {current_id, tag_id}
    assert_data_intact(instance, TABLE_NAME, 5)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_max_ref_age(started_cluster_iceberg_with_spark, storage_type):
    """Non-main branch ref with age > max-ref-age-ms is expired and removed."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_max_ref_age", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )

    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    current_id = snap_ids[-1]
    oldest_id = snap_ids[0]

    def add_short_lived_branch(meta):
        meta["refs"] = {
            "main": {"snapshot-id": current_id, "type": "branch"},
            "feature": {
                "snapshot-id": oldest_id,
                "type": "branch",
                "max-ref-age-ms": 1,
            },
        }
        meta.setdefault("properties", {}).update(AGGRESSIVE_RETENTION)
    update_iceberg_metadata(instance, TABLE_NAME, add_short_lived_branch)
    time.sleep(2)

    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    assert "feature" not in get_refs(instance, TABLE_NAME), \
        "Feature ref should be expired (age > 1ms)"
    assert get_retained_ids(instance, TABLE_NAME) == {current_id}
    assert_data_intact(instance, TABLE_NAME, 3)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_branch_min_keep_override(started_cluster_iceberg_with_spark, storage_type):
    """Branch-level min-snapshots-to-keep overrides the table default."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_branch_min_keep", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 5
    )

    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    current_id = snap_ids[-1]
    branch_head_id = snap_ids[2]

    def add_branch(meta):
        meta["refs"] = {
            "main": {"snapshot-id": current_id, "type": "branch"},
            "feature": {
                "snapshot-id": branch_head_id,
                "type": "branch",
                "min-snapshots-to-keep": 2,
            },
        }
        meta.setdefault("properties", {}).update(AGGRESSIVE_RETENTION)
    update_iceberg_metadata(instance, TABLE_NAME, add_branch)

    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    # main: min-keep=1 → current (snap5)
    # feature: min-keep=2 → snap3, snap2
    expected = {current_id, branch_head_id, snap_ids[1]}
    assert get_retained_ids(instance, TABLE_NAME) == expected, \
        f"Expected main(1) + feature(2)"
    assert_data_intact(instance, TABLE_NAME, 5)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_dangling_ref_removed(started_cluster_iceberg_with_spark, storage_type):
    """Refs pointing to non-existent snapshots are removed during expiration."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_dangling_ref", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )

    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    current_id = snap_ids[-1]

    def add_dangling_ref(meta):
        meta["refs"] = {
            "main": {"snapshot-id": current_id, "type": "branch"},
            "orphan-branch": {"snapshot-id": 999999999, "type": "branch"},
        }
        meta.setdefault("properties", {}).update(AGGRESSIVE_RETENTION)
    update_iceberg_metadata(instance, TABLE_NAME, add_dangling_ref)

    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    refs = get_refs(instance, TABLE_NAME)
    assert "orphan-branch" not in refs, "Dangling ref should be removed"
    assert "main" in refs, "Main branch should always be preserved"
    assert_data_intact(instance, TABLE_NAME, 3)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_boundary_max_ref_age(started_cluster_iceberg_with_spark, storage_type):
    """Two-phase boundary: generous max-ref-age retains ref, then 1ms expires it."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_boundary_ref_age", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )

    meta = read_iceberg_metadata(instance, TABLE_NAME)
    snap_ts = {s["snapshot-id"]: s["timestamp-ms"] for s in meta["snapshots"]}
    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    current_id = snap_ids[-1]
    oldest_id = snap_ids[0]

    now_ms = int(time.time() * 1000)
    generous_age = (now_ms - snap_ts[oldest_id]) + 10000

    def add_retained_ref(m):
        m["refs"] = {
            "main": {"snapshot-id": current_id, "type": "branch"},
            "feature": {
                "snapshot-id": oldest_id,
                "type": "branch",
                "max-ref-age-ms": generous_age,
            },
        }
        m.setdefault("properties", {}).update(AGGRESSIVE_RETENTION)
    update_iceberg_metadata(instance, TABLE_NAME, add_retained_ref)

    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)
    assert "feature" in get_refs(instance, TABLE_NAME), \
        "Ref within max-ref-age should NOT be expired"

    def expire_ref(m):
        m["refs"]["feature"]["max-ref-age-ms"] = 1
    update_iceberg_metadata(instance, TABLE_NAME, expire_ref)

    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)
    assert "feature" not in get_refs(instance, TABLE_NAME), \
        "Ref with 1ms max-ref-age should be expired"
