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
    """snapshot_id -> (made_current_at, is_current_ancestor) for every row of iceberg_history."""
    raw = instance.query(
        f"SELECT snapshot_id, toString(made_current_at), is_current_ancestor "
        f"FROM system.iceberg_history "
        f"WHERE database = 'default' AND table = '{table_name}' "
        f"ORDER BY snapshot_id FORMAT TSV"
    ).strip()
    result = {}
    for line in raw.split("\n"):
        snapshot_id, made_current_at, is_current_ancestor = line.split("\t")
        result[snapshot_id] = (made_current_at, is_current_ancestor)
    return result


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
    # None of the snapshots should sit at the epoch before expire either.
    assert all(
        made_current_at > "2000-01-01" for made_current_at, _ in before.values()
    ), f"unexpected epoch timestamp before expire: {before}"

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

    # made_current_at of every retained snapshot is unchanged (and never the epoch).
    for snapshot_id, (made_current_at, _) in after.items():
        assert made_current_at == before[snapshot_id][0], (
            f"made_current_at of retained snapshot {snapshot_id} changed after expire: "
            f"{before[snapshot_id][0]} -> {made_current_at}"
        )
        assert made_current_at > "2000-01-01", (
            f"retained snapshot {snapshot_id} shows epoch made_current_at after expire: "
            f"{made_current_at}"
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
    # made_current_at falls back to the snapshot's own timestamp-ms (never the epoch).
    for snapshot_id, (made_current_at, _) in history.items():
        assert made_current_at > "2000-01-01", (
            f"snapshot {snapshot_id} shows epoch made_current_at with snapshot-log absent: "
            f"{made_current_at}"
        )
