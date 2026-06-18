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
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"
    latest = 0
    for f in glob.glob(os.path.join(metadata_dir, "*.metadata.json")):
        m = re.search(r"v(\d+)", os.path.basename(f))
        if m:
            latest = max(latest, int(m.group(1)))
    with open(os.path.join(metadata_dir, "version-hint.text"), "w") as f:
        f.write(str(latest))


def spark_alter_table(cluster, spark, storage_type, table_name, *sql_fragments):
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


def expire_snapshots(instance, target, timestamp=None, args=None, settings=None):
    query_args = []
    if timestamp:
        query_args.append(f"expire_before = '{timestamp}'")
    if args:
        query_args.extend(args)

    settings_to_use = dict(ICEBERG_SETTINGS)
    if settings:
        settings_to_use.update(settings)

    return instance.query(
        f"ALTER TABLE {target} EXECUTE expire_snapshots({', '.join(query_args)});",
        settings=settings_to_use,
    )


def parse_expire_result(result):
    counts = {}
    for line in result.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) == 2:
            counts[parts[0]] = int(parts[1])
    return counts


def get_snapshot_ids(instance, table_name):
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


def ch_history_ids(node, table):
    out = node.query(
        f"SELECT snapshot_id FROM system.iceberg_history "
        f"WHERE table LIKE '%{table}%' ORDER BY made_current_at, snapshot_id"
    ).strip()
    return [int(line) for line in out.split("\n") if line]


def ch_history_count(node, table):
    return len(set(ch_history_ids(node, table)))


def ch_assert_data_intact(node, target, n_rows):
    assert node.query(f"SELECT count() FROM {target}").strip() == str(n_rows)


def ch_insert(node, target, value):
    node.query(f"INSERT INTO {target} VALUES ({value});", settings=ICEBERG_SETTINGS)


def build_table(engine, node, storage_type, n_rows, format_version=2):
    table = engine.unique_table("test_expire")
    engine.create_table(table, [("x", "int")], format_version=format_version)
    for val in range(1, n_rows + 1):
        engine.insert(table, [(val,)])
    engine.sync(table, storage_type)
    target = engine.clickhouse_read_target(node, table, storage_type)
    return table, target


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_basic(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table, target = build_table(engine, node, storage_type, 3)

    ch_assert_data_intact(node, target, 3)
    assert ch_history_count(node, table) >= 3

    time.sleep(2)
    expire_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    ch_insert(node, target, 4)

    result = expire_snapshots(node, target, expire_timestamp)
    counts = parse_expire_result(result)
    assert len(counts) == 7, f"Expected 7 metrics, got {counts}"
    assert all(v >= 0 for v in counts.values()), f"All counts should be non-negative, got {counts}"
    ch_assert_data_intact(node, target, 4)


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_positional_timestamp(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table, target = build_table(engine, node, storage_type, 3)

    time.sleep(2)
    expire_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    ch_insert(node, target, 4)

    result = node.query(
        f"ALTER TABLE {target} EXECUTE expire_snapshots('{expire_timestamp}');",
        settings=ICEBERG_SETTINGS,
    )
    counts = parse_expire_result(result)
    assert len(counts) == 7, f"Expected 7 metrics, got {counts}"
    assert all(v >= 0 for v in counts.values()), f"All counts should be non-negative, got {counts}"
    ch_assert_data_intact(node, target, 4)


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_no_expirable(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table, target = build_table(engine, node, storage_type, 1)
    result = expire_snapshots(node, target, "2020-01-01 00:00:00")
    counts = parse_expire_result(result)
    assert all(v == 0 for v in counts.values()), f"Expected all zeros for no-op, got {counts}"
    ch_assert_data_intact(node, target, 1)


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_format_v1_error(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table, target = build_table(engine, node, storage_type, 1, format_version=1)

    error = node.query_and_get_error(
        f"ALTER TABLE {target} EXECUTE expire_snapshots(expire_before = '2020-01-01 00:00:00');",
        settings=ICEBERG_SETTINGS,
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS error for v1, got: {error}"
    assert "second version" in error, f"Expected v2-only message, got: {error}"


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_preserves_current(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table, target = build_table(engine, node, storage_type, 2)
    result = expire_snapshots(node, target, FAR_FUTURE)
    counts = parse_expire_result(result)
    assert counts["deleted_data_files_count"] >= 0
    assert counts["deleted_manifest_lists_count"] >= 0
    ch_assert_data_intact(node, target, 2)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_expire_snapshots_no_args_default_retention(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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
def test_expire_snapshots_retain_last(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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
def test_expire_snapshots_retention_period(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_dry_run(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table, target = build_table(engine, node, storage_type, 4)
    time.sleep(2)
    history_before = ch_history_count(node, table)
    ids_before = set(ch_history_ids(node, table))

    result = expire_snapshots(
        node,
        target,
        FAR_FUTURE,
        args=["retention_period = '1ms'", "retain_last = 1", "dry_run = 1"],
    )
    counts = parse_expire_result(result)

    assert counts["dry_run"] == 1
    assert counts["deleted_manifest_lists_count"] > 0
    assert ch_history_count(node, table) == history_before
    assert set(ch_history_ids(node, table)) == ids_before
    ch_assert_data_intact(node, target, 4)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_expire_snapshots_server_default_override(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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
def test_expire_snapshots_snapshot_ids(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_snapshot_ids_cannot_expire_current(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table, target = build_table(engine, node, storage_type, 3)
    current_id = ch_history_ids(node, table)[-1]

    error = node.query_and_get_error(
        f"ALTER TABLE {target} EXECUTE expire_snapshots(snapshot_ids = [{current_id}]);",
        settings=ICEBERG_SETTINGS,
    )
    assert "cannot expire snapshot" in error


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_snapshot_ids_rejects_incompatible_args(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table, target = build_table(engine, node, storage_type, 3)
    oldest_id = ch_history_ids(node, table)[0]

    error = node.query_and_get_error(
        f"ALTER TABLE {target} EXECUTE expire_snapshots(snapshot_ids = [{oldest_id}], retain_last = 1);",
        settings=ICEBERG_SETTINGS,
    )
    assert "cannot be combined" in error


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_snapshot_ids_nonexistent_error(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table, target = build_table(engine, node, storage_type, 3)

    error = node.query_and_get_error(
        f"ALTER TABLE {target} EXECUTE expire_snapshots(snapshot_ids = [999999999999]);",
        settings=ICEBERG_SETTINGS,
    )
    assert "does not exist" in error


@pytest.mark.parametrize("storage_type", ["s3"])
def test_expire_snapshots_requires_experimental_setting(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table, target = build_table(engine, node, storage_type, 2)

    settings_no_flag = {"allow_insert_into_iceberg": 1, "allow_experimental_expire_snapshots": 0}
    error = node.query_and_get_error(
        f"ALTER TABLE {target} EXECUTE expire_snapshots();",
        settings=settings_no_flag,
    )
    assert "SUPPORT_IS_DISABLED" in error, f"Expected SUPPORT_IS_DISABLED, got: {error}"
    assert "allow_experimental_expire_snapshots" in error, \
        f"Error should mention the setting name, got: {error}"

    result = expire_snapshots(node, target)
    parse_expire_result(result)
    ch_assert_data_intact(node, target, 2)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_files_cleaned(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_expire_snapshots_retention_min_keep(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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
def test_expire_snapshots_retention_max_age(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_expire_snapshots_no_args_with_short_max_age(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_snapshot_ids_cannot_expire_ref_head(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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
def test_expire_snapshots_boundary_max_age(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_tag_retained(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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
def test_expire_snapshots_branch_min_keep_override(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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

    expected = {current_id, branch_head_id, snap_ids[1]}
    assert get_retained_ids(instance, TABLE_NAME) == expected, \
        f"Expected main(1) + feature(2)"
    assert_data_intact(instance, TABLE_NAME, 5)


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_max_ref_age(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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
def test_expire_snapshots_dangling_ref_removed(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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
def test_expire_snapshots_boundary_max_ref_age(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
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


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_shared_manifest_no_double_count(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_shared_manifest", storage_type)

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

    reported_avro_total = counts["deleted_manifest_files_count"] + counts["deleted_manifest_lists_count"]
    assert reported_avro_total == len(deleted_avro), (
        f"Double-counting detected: reported {reported_avro_total} total .avro deletions "
        f"(manifest_files={counts['deleted_manifest_files_count']}, "
        f"manifest_lists={counts['deleted_manifest_lists_count']}) "
        f"but only {len(deleted_avro)} .avro files were actually removed"
    )
    assert counts["deleted_manifest_lists_count"] >= 1, \
        "Expected at least one manifest list deleted (ML1 shared by S1 and S2)"


@pytest.mark.parametrize("storage_type", ["local"])
def test_expire_snapshots_snapshot_ids_with_fuse(spark_only, storage_type):
    started_cluster_iceberg_with_spark = spark_only.cluster
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = make_table_name("test_expire_ids_fuse", storage_type)

    create_and_populate(
        started_cluster_iceberg_with_spark, instance, storage_type, TABLE_NAME, 1
    )
    time.sleep(2)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (2);", settings=ICEBERG_SETTINGS)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (3);", settings=ICEBERG_SETTINGS)

    snap_ids = get_snapshot_ids(instance, TABLE_NAME)
    assert len(snap_ids) == 3
    s1_id, s2_id, s3_id = snap_ids

    meta = read_iceberg_metadata(instance, TABLE_NAME)
    ts = {s["snapshot-id"]: s["timestamp-ms"] for s in meta["snapshots"]}
    fuse_s = ts[s1_id] // 1000 + 1
    fuse_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(fuse_s))

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
