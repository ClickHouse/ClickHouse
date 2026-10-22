"""OPTIMIZE (data compaction) that loses the commit race retries on top of the snapshot that won it."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str

FAILPOINT = "iceberg_compaction_pause_before_metadata_commit"
WRITE_SETTINGS = {"allow_insert_into_iceberg": 1, "mutations_sync": 2}


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_optimize_commit_retry(started_cluster_iceberg_with_spark, storage_type):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    table_name = f"test_optimize_commit_retry_{storage_type}_{get_uuid_str()}"

    create_iceberg_table(storage_type, instance, table_name, cluster, schema="(a Int64)", format_version=2)
    instance.query(f"INSERT INTO {table_name} VALUES (1), (2), (3)", settings=WRITE_SETTINGS)
    instance.query(f"ALTER TABLE {table_name} DELETE WHERE a = 2", settings=WRITE_SETTINGS)

    def snapshots():
        return instance.query(
            f"SELECT snapshot_id, parent_id, operation FROM system.iceberg_history "
            f"WHERE database = currentDatabase() AND table = '{table_name}' ORDER BY made_current_at"
        ).strip().splitlines()

    instance.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        optimize = executor.submit(
            instance.query,
            f"OPTIMIZE TABLE {table_name}",
            settings={"allow_experimental_iceberg_compaction": 1},
            timeout=120,
        )
        instance.query(f"SYSTEM WAIT FAILPOINT {FAILPOINT} PAUSE", timeout=60)

        # Commit while OPTIMIZE is paused, so that its first commit attempt fails.
        instance.query(f"INSERT INTO {table_name} VALUES (4)", settings=WRITE_SETTINGS)
        insert_snapshot_id = snapshots()[-1].split("\t")[0]

        instance.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        optimize.result(timeout=120)
    finally:
        # A failpoint left enabled would pause the next test. Disabling it twice is harmless.
        instance.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        executor.shutdown(wait=False)

    _, parent_id, operation = snapshots()[-1].split("\t")
    assert (parent_id, operation) == (insert_snapshot_id, "REPLACE")
    assert instance.query(f"SELECT a FROM {table_name} ORDER BY a") == "1\n3\n4\n"
    assert instance.query(
        f"SELECT countIf(content = 'POSITION_DELETE') FROM system.iceberg_files "
        f"WHERE database = currentDatabase() AND table = '{table_name}'"
    ) == "0\n"
