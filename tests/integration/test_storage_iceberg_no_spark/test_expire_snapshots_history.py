import json
import re

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)

# expire_snapshots and Iceberg mutations require format version 2.
EXPIRE_SETTINGS = {"allow_insert_into_iceberg": 1, "allow_experimental_expire_snapshots": 1}


def _metadata_dir(table_name):
    return f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"


def _read_latest_metadata(instance, table_name):
    metadata_dir = _metadata_dir(table_name)
    latest = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    return json.loads(instance.exec_in_container(["cat", latest])), latest


def _write_next_metadata(instance, table_name, meta, prev_path):
    metadata_dir = _metadata_dir(table_name)
    new_version = int(re.search(r"/v(\d+)[^/]*\.metadata\.json$", prev_path).group(1)) + 1
    new_path = f"{metadata_dir}/v{new_version}.metadata.json"
    instance.exec_in_container(
        ["bash", "-c", f"cat > {new_path} << 'JSONEOF'\n{json.dumps(meta, indent=4)}\nJSONEOF"]
    )


def _history(instance, table_name):
    """snapshot_id -> (made_current_at_ms, is_current_ancestor) for every row of iceberg_history.

    `made_current_at` is read as unix milliseconds so it can be compared against the raw
    `timestamp-ms` of the metadata it is supposed to come from. That round-trips exactly: the
    column is DateTime64(6) and Iceberg timestamps are whole milliseconds.
    """
    raw = instance.query(
        f"SELECT snapshot_id, toUnixTimestamp64Milli(made_current_at), is_current_ancestor "
        f"FROM system.iceberg_history "
        f"WHERE database = 'default' AND table = '{table_name}' "
        f"ORDER BY snapshot_id FORMAT TSV"
    ).strip()
    result = {}
    for line in raw.split("\n"):
        snapshot_id, made_current_at_ms, is_current_ancestor = line.split("\t")
        result[snapshot_id] = (int(made_current_at_ms), is_current_ancestor)
    return result


def _expected_made_current_at_ms(meta):
    """snapshot_id -> the timestamp-ms `made_current_at` must report, per the documented contract.

    The snapshot-log entry wins when the snapshot is still listed there; otherwise the value falls
    back to the snapshot's own commit timestamp.
    """
    snapshot_ts = {str(s["snapshot-id"]): s["timestamp-ms"] for s in meta["snapshots"]}
    log_ts = {str(e["snapshot-id"]): e["timestamp-ms"] for e in meta.get("snapshot-log", [])}
    return {sid: log_ts.get(sid, ts) for sid, ts in snapshot_ts.items()}, snapshot_ts, log_ts


def test_iceberg_history_made_current_at_stable_after_expire(
    started_cluster_iceberg_no_spark,
):
    """Expiring a snapshot must not rewrite made_current_at of the RETAINED snapshots.

    Regression for https://github.com/ClickHouse/ClickHouse/issues/111734: expiring a middle
    snapshot trims the metadata snapshot-log to its retained suffix, so an older retained
    snapshot drops out of the log. system.iceberg_history read made_current_at only from the
    snapshot-log, so such a snapshot showed the epoch (1970-01-01) instead of its real commit
    time. The fix falls back to the snapshot's own timestamp-ms.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_expire_history_" + get_uuid_str()

    create_iceberg_table(
        "local",
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int)",
        format_version=2,
    )

    # A (APPEND) -> B (OVERWRITE via delete) -> C (APPEND, current).
    instance.query(f"INSERT INTO {table_name} VALUES (1), (2), (3);")
    instance.query(
        f"ALTER TABLE {table_name} DELETE WHERE 1;",
        settings={"mutations_sync": 2, **EXPIRE_SETTINGS},
    )
    instance.query(f"INSERT INTO {table_name} VALUES (4), (5);")

    before = _history(instance, table_name)
    assert len(before) == 3, f"expected three snapshots before expire, got: {before}"
    # Every snapshot already reports its own metadata timestamp before expire.
    meta_before, _ = _read_latest_metadata(instance, table_name)
    expected_before, _, _ = _expected_made_current_at_ms(meta_before)
    assert {sid: ms for sid, (ms, _) in before.items()} == expected_before, (
        f"made_current_at does not match the metadata timestamps before expire: "
        f"{before} vs {expected_before}"
    )

    # Expire the middle snapshot B (the OVERWRITE). It is an ancestor's child, so trimming the
    # snapshot-log removes the oldest APPEND (A) from the log while keeping it in `snapshots`.
    ids = instance.query(
        f"SELECT snapshot_id FROM system.iceberg_history "
        f"WHERE database = 'default' AND table = '{table_name}' "
        f"AND operation = 'OVERWRITE' FORMAT TSV"
    ).strip().split("\n")
    assert len(ids) == 1 and ids[0], f"expected exactly one OVERWRITE snapshot, got: {ids}"
    b_id = ids[0]

    instance.query(
        f"ALTER TABLE {table_name} EXECUTE expire_snapshots(snapshot_ids = [{b_id}]);",
        settings=EXPIRE_SETTINGS,
    )

    after = _history(instance, table_name)

    # B is expired and gone; the other two snapshots remain.
    assert b_id not in after, f"expired snapshot {b_id} still present: {after}"
    assert set(after) == set(before) - {b_id}, f"unexpected surviving set: {after}"

    # The premise of the regression: expiring B trims the snapshot-log so that a RETAINED snapshot
    # is no longer listed there and can only be dated from its own timestamp-ms. Without this the
    # test below would pass while never exercising the fallback.
    meta_after, _ = _read_latest_metadata(instance, table_name)
    expected_after, snapshot_ts_after, log_ts_after = _expected_made_current_at_ms(meta_after)
    dropped_from_log = set(snapshot_ts_after) - set(log_ts_after)
    assert dropped_from_log, (
        f"expected a retained snapshot to drop out of the snapshot-log after expiring {b_id}; "
        f"snapshots={sorted(snapshot_ts_after)} log={sorted(log_ts_after)}"
    )

    # made_current_at of every retained snapshot is unchanged, and equals the exact timestamp-ms it
    # must come from - the snapshot's own for the one dropped from the log, the log entry otherwise.
    for snapshot_id, (made_current_at_ms, _) in after.items():
        assert made_current_at_ms == before[snapshot_id][0], (
            f"made_current_at of retained snapshot {snapshot_id} changed after expire: "
            f"{before[snapshot_id][0]} -> {made_current_at_ms}"
        )
        assert made_current_at_ms == expected_after[snapshot_id], (
            f"made_current_at of retained snapshot {snapshot_id} is {made_current_at_ms}, "
            f"expected timestamp-ms {expected_after[snapshot_id]} "
            f"({'own snapshot' if snapshot_id in dropped_from_log else 'snapshot-log'})"
        )


def test_iceberg_history_without_snapshot_log(started_cluster_iceberg_no_spark):
    """system.iceberg_history must still report snapshots when snapshot-log is absent.

    snapshot-log is optional in the Iceberg spec (and expire_snapshots may drop it entirely).
    getHistory extracted it unconditionally, which threw and made the system-table reader omit
    the whole table. The snapshot's own timestamp-ms is used for made_current_at instead.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_history_no_snapshot_log_" + get_uuid_str()

    create_iceberg_table(
        "local",
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int)",
        format_version=2,
    )
    instance.query(f"INSERT INTO {table_name} VALUES (1), (2);")

    meta, prev = _read_latest_metadata(instance, table_name)
    assert meta.get("snapshots"), "snapshot must be present after INSERT"
    assert "snapshot-log" in meta, "expected snapshot-log before removing it"
    snapshot_ts = {str(s["snapshot-id"]): s["timestamp-ms"] for s in meta["snapshots"]}
    meta.pop("snapshot-log")
    _write_next_metadata(instance, table_name, meta, prev)

    history = _history(instance, table_name)
    # The table is reported (not silently dropped) and every snapshot is present.
    assert set(history) == set(snapshot_ts), (
        f"expected all snapshots reported with snapshot-log absent, got: {history}"
    )
    # made_current_at is exactly the snapshot's own timestamp-ms, not merely some non-epoch value.
    for snapshot_id, (made_current_at_ms, _) in history.items():
        assert made_current_at_ms == snapshot_ts[snapshot_id], (
            f"snapshot {snapshot_id} reports made_current_at {made_current_at_ms} with "
            f"snapshot-log absent, expected its own timestamp-ms {snapshot_ts[snapshot_id]}"
        )
