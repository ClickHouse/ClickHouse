import glob
import json
import os
import re
import time

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    default_upload_directory,
    get_uuid_str,
)


ICEBERG_SETTINGS = {"allow_insert_into_iceberg": 1, "allow_experimental_expire_snapshots": 1}
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
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    raw = instance.exec_in_container(["cat", latest])
    return json.loads(raw), latest


def _write_iceberg_metadata(instance, table_name, meta, prev_path):
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


def _fix_version_hint_for_spark(table_name):
    """Rewrite version-hint.text as a plain version number.
    ClickHouse writes the full filename (e.g. 'v3.metadata.json');
    Spark's Hadoop catalog expects just the number (e.g. '3').
    """
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    latest = 0
    for f in glob.glob(os.path.join(metadata_dir, "*.metadata.json")):
        m = re.search(r"v(\d+)", os.path.basename(f))
        if m:
            latest = max(latest, int(m.group(1)))
    with open(os.path.join(metadata_dir, "version-hint.text"), "w") as f:
        f.write(str(latest))


def spark_alter_table(cluster, spark, storage_type, table_name, *sql_fragments):
    """Execute Spark SQL ALTER TABLE on a ClickHouse-created Iceberg table.

    Downloads the table from storage to the host (so Spark can see it),
    executes the SQL statements, then uploads the result back.

    Each sql_fragment is appended to 'ALTER TABLE {table_name} '.
    Example: spark_alter_table(..., "SET TBLPROPERTIES('key' = 'val')")
    """
    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"
    default_download_directory(cluster, storage_type, table_dir, table_dir)
    _fix_version_hint_for_spark(table_name)
    for fragment in sql_fragments:
        spark.sql(f"ALTER TABLE {table_name} {fragment}")
    default_upload_directory(cluster, storage_type, table_dir, table_dir)


def create_and_populate(cluster, instance, storage_type, table_name, n_rows, format_version=2):
    create_iceberg_table(
        storage_type, instance, table_name, cluster, "(x Int)", format_version
    )
    for val in range(1, n_rows + 1):
        instance.query(
            f"INSERT INTO {table_name} VALUES ({val});",
            settings=ICEBERG_SETTINGS,
        )


def expire_snapshots(instance, table_name, timestamp=None, args=None, settings=None):
    query_args = []
    if timestamp:
        query_args.append(f"expire_before = '{timestamp}'")
    if args:
        query_args.extend(args)

    settings_to_use = dict(ICEBERG_SETTINGS)
    if settings:
        settings_to_use.update(settings)

    return instance.query(
        f"ALTER TABLE {table_name} EXECUTE expire_snapshots({', '.join(query_args)});",
        settings=settings_to_use,
    )


def parse_expire_result(result):
    """Parse the metric_name/metric_value rows from expire_snapshots into a dict."""
    counts = {}
    for line in result.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) == 2:
            counts[parts[0]] = int(parts[1])
    return counts


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

@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
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

    result = expire_snapshots(instance, TABLE_NAME, expire_timestamp)
    counts = parse_expire_result(result)
    assert len(counts) == 7, f"Expected 7 metrics, got {counts}"
    assert all(v >= 0 for v in counts.values()), f"All counts should be non-negative, got {counts}"
    assert_data_intact(instance, TABLE_NAME, 4)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_expire_snapshots_positional_timestamp(started_cluster_iceberg_with_spark, storage_type):
    """Legacy positional timestamp syntax works at runtime (backward compat)."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_positional", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )

    time.sleep(2)
    expire_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (4);", settings=ICEBERG_SETTINGS
    )

    result = instance.query(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots('{expire_timestamp}');",
        settings=ICEBERG_SETTINGS,
    )
    counts = parse_expire_result(result)
    assert len(counts) == 7, f"Expected 7 metrics, got {counts}"
    assert all(v >= 0 for v in counts.values()), f"All counts should be non-negative, got {counts}"
    assert_data_intact(instance, TABLE_NAME, 4)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_expire_snapshots_no_expirable(started_cluster_iceberg_with_spark, storage_type):
    """No-op when no snapshots match the criteria."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_no_expirable", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1
    )
    result = expire_snapshots(instance, TABLE_NAME, "2020-01-01 00:00:00")
    counts = parse_expire_result(result)
    assert all(v == 0 for v in counts.values()), f"Expected all zeros for no-op, got {counts}"
    assert_data_intact(instance, TABLE_NAME, 1)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_expire_snapshots_format_v1_error(started_cluster_iceberg_with_spark, storage_type):
    """expire_snapshots rejects format version 1 with BAD_ARGUMENTS."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_v1_error", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1,
        format_version=1,
    )

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots(expire_before = '2020-01-01 00:00:00');",
        settings=ICEBERG_SETTINGS,
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS error for v1, got: {error}"
    assert "second version" in error, f"Expected v2-only message, got: {error}"


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_expire_snapshots_preserves_current(started_cluster_iceberg_with_spark, storage_type):
    """Current snapshot is never expired, even with a far-future timestamp."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_preserves_current", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 2
    )
    result = expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)
    counts = parse_expire_result(result)
    assert counts["deleted_data_files_count"] >= 0
    assert counts["deleted_manifest_lists_count"] >= 0
    assert_data_intact(instance, TABLE_NAME, 2)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_files_cleaned(started_cluster_iceberg_with_spark, storage_type):
    """Expired snapshot files are physically removed; retained snapshot files survive.
    Local-only: needs direct filesystem access to verify file deletion.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = make_table_name("test_expire_files_cleaned", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 2
    )

    time.sleep(2)
    expire_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (3);", settings=ICEBERG_SETTINGS
    )

    spark_alter_table(
        started_cluster_iceberg_with_spark, spark, storage_type, TABLE_NAME,
        f"SET TBLPROPERTIES('history.expire.max-snapshot-age-ms' = '1', "
        f"'history.expire.min-snapshots-to-keep' = '1')",
    )

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

    result = expire_snapshots(instance, TABLE_NAME, expire_timestamp)
    counts = parse_expire_result(result)

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

    assert counts["deleted_manifest_lists_count"] > 0, \
        f"Expected deleted manifest lists, got {counts}"
    assert sum(counts.values()) > 0, f"Expected some files to be deleted, got {counts}"

    assert_data_intact(instance, TABLE_NAME, 3)


# ---------------------------------------------------------------------------
# Retention policy tests (table-level properties via Spark SQL)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_expire_snapshots_retention_min_keep(started_cluster_iceberg_with_spark, storage_type):
    """min-snapshots-to-keep prevents expiring the N most recent ancestors."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = make_table_name("test_expire_min_keep", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 5
    )
    assert get_history_count(instance, TABLE_NAME) == 5

    spark_alter_table(
        started_cluster_iceberg_with_spark, spark, storage_type, TABLE_NAME,
        "SET TBLPROPERTIES('history.expire.min-snapshots-to-keep' = '3', "
        "'history.expire.max-snapshot-age-ms' = '1')",
    )
    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    assert get_history_count(instance, TABLE_NAME) == 3
    assert_data_intact(instance, TABLE_NAME, 5)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_expire_snapshots_retention_max_age(started_cluster_iceberg_with_spark, storage_type):
    """max-snapshot-age-ms preserves recent snapshots regardless of timestamp cutoff."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = make_table_name("test_expire_max_age", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 5
    )

    spark_alter_table(
        started_cluster_iceberg_with_spark, spark, storage_type, TABLE_NAME,
        f"SET TBLPROPERTIES('history.expire.min-snapshots-to-keep' = '1', "
        f"'history.expire.max-snapshot-age-ms' = '{3600 * 1000}')",
    )
    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    assert get_history_count(instance, TABLE_NAME) == 5, \
        "All 5 snapshots are within 1 hour, all should be retained"
    assert_data_intact(instance, TABLE_NAME, 5)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
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


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_expire_snapshots_no_args_with_short_max_age(started_cluster_iceberg_with_spark, storage_type):
    """No-arg expire with very short max-age expires old snapshots."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = make_table_name("test_expire_no_args_short_age", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 5
    )
    time.sleep(2)

    spark_alter_table(
        started_cluster_iceberg_with_spark, spark, storage_type, TABLE_NAME,
        "SET TBLPROPERTIES('history.expire.max-snapshot-age-ms' = '1', "
        "'history.expire.min-snapshots-to-keep' = '1')",
    )
    expire_snapshots(instance, TABLE_NAME)

    assert get_history_count(instance, TABLE_NAME) == 1, \
        "With 1ms max-age and min-keep=1, only current snapshot should remain"
    assert_data_intact(instance, TABLE_NAME, 5)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_expire_snapshots_retain_last(started_cluster_iceberg_with_spark, storage_type):
    """retain_last keeps the most recent N snapshots for this invocation."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_retain_last", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 5
    )
    time.sleep(2)

    expire_snapshots(
        instance,
        TABLE_NAME,
        FAR_FUTURE,
        args=["retain_last = 2", "retention_period = '1ms'"],
    )

    assert get_history_count(instance, TABLE_NAME) == 2
    assert_data_intact(instance, TABLE_NAME, 5)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_expire_snapshots_retention_period(started_cluster_iceberg_with_spark, storage_type):
    """retention_period overrides max-snapshot-age-ms for this invocation."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_retention_period", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 4
    )
    time.sleep(2)

    expire_snapshots(
        instance,
        TABLE_NAME,
        FAR_FUTURE,
        args=["retention_period = '1ms'"],
    )

    assert get_history_count(instance, TABLE_NAME) == 1
    assert_data_intact(instance, TABLE_NAME, 4)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_snapshot_ids(started_cluster_iceberg_with_spark, storage_type):
    """snapshot_ids expires only explicitly selected snapshots."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_snapshot_ids", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 4
    )

    snapshot_ids = get_snapshot_ids(instance, TABLE_NAME)
    current_id = snapshot_ids[-1]
    to_expire = snapshot_ids[0]

    expire_snapshots(
        instance,
        TABLE_NAME,
        args=[f"snapshot_ids = [{to_expire}]"],
    )

    retained = get_retained_ids(instance, TABLE_NAME)
    assert to_expire not in retained
    assert current_id in retained
    assert len(retained) == 3
    assert_data_intact(instance, TABLE_NAME, 4)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_snapshot_ids_cannot_expire_current(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_snapshot_ids_current", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )
    current_id = get_snapshot_ids(instance, TABLE_NAME)[-1]

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots(snapshot_ids = [{current_id}]);",
        settings=ICEBERG_SETTINGS,
    )
    assert "cannot expire snapshot" in error


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_snapshot_ids_rejects_incompatible_args(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_snapshot_ids_incompatible", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )
    oldest_id = get_snapshot_ids(instance, TABLE_NAME)[0]

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots(snapshot_ids = [{oldest_id}], retain_last = 1);",
        settings=ICEBERG_SETTINGS,
    )
    assert "cannot be combined" in error


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_snapshot_ids_nonexistent_error(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_snapshot_ids_nonexistent", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots(snapshot_ids = [999999999999]);",
        settings=ICEBERG_SETTINGS,
    )
    assert "does not exist" in error


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_snapshot_ids_cannot_expire_ref_head(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = make_table_name("test_expire_snapshot_ids_ref_head", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 4
    )
    tagged_id = get_snapshot_ids(instance, TABLE_NAME)[1]

    spark_alter_table(
        started_cluster_iceberg_with_spark,
        spark,
        storage_type,
        TABLE_NAME,
        f"CREATE TAG `release_protected` AS OF VERSION {tagged_id}",
    )

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots(snapshot_ids = [{tagged_id}]);",
        settings=ICEBERG_SETTINGS,
    )
    assert "cannot expire snapshot" in error


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_dry_run(started_cluster_iceberg_with_spark, storage_type):
    """dry_run reports deletions but does not modify metadata or delete files."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_dry_run", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 4
    )
    time.sleep(2)
    history_before = get_history_count(instance, TABLE_NAME)
    retained_before = get_retained_ids(instance, TABLE_NAME)

    result = expire_snapshots(
        instance,
        TABLE_NAME,
        FAR_FUTURE,
        args=["retention_period = '1ms'", "retain_last = 1", "dry_run = 1"],
    )
    counts = parse_expire_result(result)

    assert counts["dry_run"] == 1
    assert counts["deleted_manifest_lists_count"] > 0
    assert get_history_count(instance, TABLE_NAME) == history_before
    assert get_retained_ids(instance, TABLE_NAME) == retained_before
    assert_data_intact(instance, TABLE_NAME, 4)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_expire_snapshots_server_default_override(started_cluster_iceberg_with_spark, storage_type):
    """Session settings override default retention when table properties are absent."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_server_defaults", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 4
    )
    time.sleep(2)

    expire_snapshots(
        instance,
        TABLE_NAME,
        settings={"iceberg_expire_default_max_snapshot_age_ms": 1},
    )

    assert get_history_count(instance, TABLE_NAME) == 1
    assert_data_intact(instance, TABLE_NAME, 4)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_boundary_max_age(started_cluster_iceberg_with_spark, storage_type):
    """Two-phase boundary: generous max-age retains all, then 1ms max-age expires old.
    Local-only: needs read_iceberg_metadata for snapshot timestamps.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = make_table_name("test_expire_boundary_age", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )

    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    oldest_id = snap_ids[0]

    spark_alter_table(
        started_cluster_iceberg_with_spark, spark, storage_type, TABLE_NAME,
        f"SET TBLPROPERTIES('history.expire.max-snapshot-age-ms' = '{3600 * 1000}', "
        f"'history.expire.min-snapshots-to-keep' = '1')",
    )

    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    retained = get_retained_ids(instance, TABLE_NAME)
    assert oldest_id in retained, \
        f"Snapshot within max-age should be retained, but {oldest_id} expired"
    assert len(retained) == 3, f"All 3 within max-age, got {len(retained)}"

    spark_alter_table(
        started_cluster_iceberg_with_spark, spark, storage_type, TABLE_NAME,
        "SET TBLPROPERTIES('history.expire.max-snapshot-age-ms' = '1', "
        "'history.expire.min-snapshots-to-keep' = '1')",
    )
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
    """A tag's snapshot is retained even when age/min-keep would otherwise expire it.
    Local-only: needs get_snapshot_ids / get_retained_ids which read container metadata.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = make_table_name("test_expire_tag_retained", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 5
    )

    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    current_id = snap_ids[-1]
    tag_id = snap_ids[1]

    spark_alter_table(
        started_cluster_iceberg_with_spark, spark, storage_type, TABLE_NAME,
        "SET TBLPROPERTIES('history.expire.max-snapshot-age-ms' = '1', "
        "'history.expire.min-snapshots-to-keep' = '1')",
        f"CREATE TAG `release_v1` AS OF VERSION {tag_id}",
    )

    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    assert get_retained_ids(instance, TABLE_NAME) == {current_id, tag_id}
    assert_data_intact(instance, TABLE_NAME, 5)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_branch_min_keep_override(started_cluster_iceberg_with_spark, storage_type):
    """Branch-level min-snapshots-to-keep overrides the table default.
    Local-only: needs get_snapshot_ids / get_retained_ids which read container metadata.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = make_table_name("test_expire_branch_min_keep", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 5
    )

    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    current_id = snap_ids[-1]
    branch_head_id = snap_ids[2]

    spark_alter_table(
        started_cluster_iceberg_with_spark, spark, storage_type, TABLE_NAME,
        "SET TBLPROPERTIES('history.expire.max-snapshot-age-ms' = '1', "
        "'history.expire.min-snapshots-to-keep' = '1')",
        f"CREATE BRANCH `feature` AS OF VERSION {branch_head_id} "
        f"WITH SNAPSHOT RETENTION 2 SNAPSHOTS",
    )

    expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)

    # main: min-keep=1 → current (snap5)
    # feature: min-keep=2 → snap3, snap2
    expected = {current_id, branch_head_id, snap_ids[1]}
    assert get_retained_ids(instance, TABLE_NAME) == expected, \
        f"Expected main(1) + feature(2)"
    assert_data_intact(instance, TABLE_NAME, 5)


# ---------------------------------------------------------------------------
# Tests requiring manual metadata patching
# These cannot use Spark SQL because:
#   - Dangling refs: Spark validates snapshot existence on CREATE BRANCH/TAG
#   - 1ms max-ref-age-ms: Spark RETAIN clause minimum granularity is minutes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_max_ref_age(started_cluster_iceberg_with_spark, storage_type):
    """Non-main branch ref with age > max-ref-age-ms is expired and removed.
    Manual patching: Spark RETAIN has minute granularity; we need 1ms."""
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
def test_expire_snapshots_dangling_ref_removed(started_cluster_iceberg_with_spark, storage_type):
    """Refs pointing to non-existent snapshots are removed during expiration.
    Manual patching: Spark validates snapshot existence on CREATE BRANCH/TAG."""
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
    """Two-phase boundary: generous max-ref-age retains ref, then 1ms expires it.
    Manual patching: Spark RETAIN has minute granularity; we need 1ms."""
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


# ---------------------------------------------------------------------------
# Regression test: shared manifests between expired snapshots
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_shared_manifest_no_double_count(started_cluster_iceberg_with_spark, storage_type):
    """Manifest lists shared between expired snapshots are counted and deleted exactly once.

    Regression test for double-counting in collectExpiredFiles:
    - The outer loop had no dedup for manifest-list paths: if two expired snapshots
      referenced the same manifest-list, result.manifest_lists was incremented twice.
    - The inner loop had no dedup for manifest-file paths: if two expired manifest-lists
      referenced the same manifest, result.manifest_files was incremented twice.

    Setup: 3 INSERTs → S1 (ML1), S2 (ML2), S3=current (ML3). We patch S2's
    manifest-list to ML1, so both S1 and S2 reference the same file. After
    expiration ML1 must appear in deleted_manifest_lists_count exactly once.
    The sum of reported .avro deletions must equal actual deleted .avro files.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_shared_manifest", storage_type)

    # 3 INSERTs → S1 (ML1), S2 (ML2), S3=current (ML3)
    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 3
    )

    meta = read_iceberg_metadata(instance, TABLE_NAME)
    current_id = meta["current-snapshot-id"]
    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    expired_ids = [sid for sid in snap_ids if sid != current_id]
    assert len(expired_ids) >= 2, "Need at least 2 expired snapshots for this test"

    s1_id = expired_ids[0]
    s2_id = expired_ids[1]
    s1_manifest_list = next(
        s["manifest-list"] for s in meta["snapshots"] if s["snapshot-id"] == s1_id
    )

    # Patch S2 to share S1's manifest-list path.
    # expired_manifest_list_paths will now contain ML1 twice (once for S1, once for S2).
    # Without the outer-loop dedup fix, ML1 is counted/deleted twice.
    def share_s1_manifest_list(m):
        for s in m["snapshots"]:
            if s["snapshot-id"] == s2_id:
                s["manifest-list"] = s1_manifest_list
        m.setdefault("properties", {}).update(AGGRESSIVE_RETENTION)

    update_iceberg_metadata(instance, TABLE_NAME, share_s1_manifest_list)

    time.sleep(1)

    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata"
    avro_before = set(
        instance.exec_in_container(
            ["bash", "-c", f"ls {metadata_dir}/*.avro 2>/dev/null || true"]
        ).split()
    )

    result = expire_snapshots(instance, TABLE_NAME, FAR_FUTURE)
    counts = parse_expire_result(result)

    avro_after = set(
        instance.exec_in_container(
            ["bash", "-c", f"ls {metadata_dir}/*.avro 2>/dev/null || true"]
        ).split()
    )
    deleted_avro = avro_before - avro_after

    # Both manifest files and manifest lists are .avro files.
    # Their reported sum must exactly equal the actual number of deleted .avro files.
    # Double-counting of either type inflates the sum above the real count.
    reported_avro_total = counts["deleted_manifest_files_count"] + counts["deleted_manifest_lists_count"]
    assert reported_avro_total == len(deleted_avro), (
        f"Double-counting detected: reported {reported_avro_total} total .avro deletions "
        f"(manifest_files={counts['deleted_manifest_files_count']}, "
        f"manifest_lists={counts['deleted_manifest_lists_count']}) "
        f"but only {len(deleted_avro)} .avro files were actually removed"
    )
    assert counts["deleted_manifest_lists_count"] >= 1, \
        "Expected at least one manifest list deleted (ML1 shared by S1 and S2)"


# ---------------------------------------------------------------------------
# Coverage gap tests (PR #99130)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_snapshot_ids_with_fuse(started_cluster_iceberg_with_spark, storage_type):
    """snapshot_ids + expire_before: fuse protects snapshots newer than the timestamp.

    Exercises the fuse-protection branch in partitionSnapshotsByIds (Mutations.cpp:1194):
    a snapshot explicitly listed in snapshot_ids is NOT expired when its timestamp
    is >= expire_before_ms.  Only snapshots strictly older than the fuse are expired.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_ids_fuse", storage_type)

    # Create S1 first, then sleep 2 s before S2/S3 so they land in a different
    # second.  expire_before is passed at second granularity ("%Y-%m-%d %H:%M:%S"),
    # so S1 and S2 must be at least 1 second apart for the fuse to sit strictly
    # between them without truncation artifacts.
    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1
    )
    time.sleep(2)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (2);", settings=ICEBERG_SETTINGS)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (3);", settings=ICEBERG_SETTINGS)

    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    assert len(snap_ids) == 3
    s1_id, s2_id, s3_id = snap_ids

    # Read raw timestamps so we can place the fuse between S1 and S2
    meta = read_iceberg_metadata(instance, TABLE_NAME)
    ts = {s["snapshot-id"]: s["timestamp-ms"] for s in meta["snapshots"]}
    # Round S1's timestamp up to the next full second: always > ts[s1_id]
    # and always <= ts[s2_id] (which is >= ts[s1_id] + 2000 ms).
    fuse_s = ts[s1_id] // 1000 + 1
    fuse_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(fuse_s))

    # Attempt to expire both S1 and S2 explicitly, with fuse protecting S2
    # S1 timestamp_s < fuse_s  → should be expired
    # S2 timestamp_s >= fuse_s → is_protected_by_fuse = true → should be retained
    result = expire_snapshots(
        instance,
        TABLE_NAME,
        args=[f"snapshot_ids = [{s1_id}, {s2_id}]", f"expire_before = '{fuse_str}'"],
    )

    retained = get_retained_ids(instance, TABLE_NAME)
    assert s1_id not in retained, "S1 (older than fuse) should have been expired"
    assert s2_id in retained, "S2 (newer than fuse) should be fuse-protected"
    assert s3_id in retained, "Current snapshot must always be retained"
    assert_data_intact(instance, TABLE_NAME, 3)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_requires_experimental_setting(started_cluster_iceberg_with_spark, storage_type):
    """expire_snapshots raises SUPPORT_IS_DISABLED when allow_experimental_expire_snapshots=0.

    Exercises the feature gate at IcebergMetadata.cpp:605-611.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_gate", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 2
    )

    # Without the experimental flag, expire_snapshots must be blocked
    settings_no_flag = {"allow_insert_into_iceberg": 1, "allow_experimental_expire_snapshots": 0}
    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} EXECUTE expire_snapshots();",
        settings=settings_no_flag,
    )
    assert "SUPPORT_IS_DISABLED" in error, f"Expected SUPPORT_IS_DISABLED, got: {error}"
    assert "allow_experimental_expire_snapshots" in error, \
        f"Error should mention the setting name, got: {error}"

    # With the flag enabled it should succeed
    result = expire_snapshots(instance, TABLE_NAME)
    parse_expire_result(result)
    assert_data_intact(instance, TABLE_NAME, 2)
