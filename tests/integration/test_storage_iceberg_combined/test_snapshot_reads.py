import time
from datetime import datetime, timezone

import pytest


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_snapshot_reads(engine, node, format_version, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_snapshot_reads_{format_version}_{storage_type}")
    engine.create_table(table, [("a", "long"), ("b", "string")], format_version=format_version)

    snapshots = []
    timestamps = []
    for start in (0, 100, 200):
        engine.insert(table, [(i, str(i + 1)) for i in range(start, start + 100)])
        engine.sync(table, storage_type)
        snapshots.append(engine.snapshot_ids(table)[-1])
        timestamps.append(int(datetime.now(timezone.utc).timestamp() * 1000))
        time.sleep(0.1)

    target = engine.clickhouse_read_target(node, table, storage_type)

    assert int(node.query(f"SELECT count() FROM {target}").strip()) == 300
    assert node.query(f"SELECT a, b FROM {target} ORDER BY 1") == node.query(
        "SELECT number, toString(number + 1) FROM numbers(300)"
    )

    for snapshot_id, expected in zip(snapshots, (100, 200, 300)):
        got = node.query(
            f"SELECT count() FROM {target}",
            settings={"iceberg_snapshot_id": snapshot_id},
        ).strip()
        assert int(got) == expected

    for ts_ms, expected in zip(timestamps, (100, 200, 300)):
        got = node.query(
            f"SELECT count() FROM {target}",
            settings={"iceberg_timestamp_ms": ts_ms},
        ).strip()
        assert int(got) == expected
