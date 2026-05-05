import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_partition_concurrent_insert_does_not_drop_new_file(started_cluster_iceberg_no_spark, storage_type):
    """The DROP PARTITION executor locks the set of target file paths on its
    first attempt and never refreshes them on retry. So a file added by a
    concurrent INSERT after DROP PARTITION has begun discovery must survive
    the operation even when both touch the same partition.

    This test pauses the executor right after discovery, runs an INSERT into
    the same partition, then lets the executor proceed. The retry loop sees
    a fresh parent snapshot but the locked target set still contains only
    the pre-INSERT files, so the new file survives."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = f"test_drop_partition_race_{storage_type}_{get_uuid_str()}"

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        schema="(a Int64, b String)",
        partition_by="(identity(a))",
        format_version=2,
    )

    instance.query(
        f"INSERT INTO {table_name} VALUES (1, 'before-drop-1'), (1, 'before-drop-2'), (2, 'keep')",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT count() FROM {table_name}").strip() == "3"

    instance.query("SYSTEM ENABLE FAILPOINT iceberg_drop_partition_pause_after_discovery")

    executor = ThreadPoolExecutor(max_workers=3)
    try:
        # 1. Wait for the executor to pause at the failpoint.
        wait_future = executor.submit(
            lambda: instance.query(
                "SYSTEM WAIT FAILPOINT iceberg_drop_partition_pause_after_discovery PAUSE",
                timeout=60,
            )
        )

        # 2. Start the DROP PARTITION; it pauses right after discovering files in partition a=1.
        drop_future = executor.submit(
            lambda: instance.query(
                f"ALTER TABLE {table_name} DROP PARTITION 1",
                settings={"allow_insert_into_iceberg": 1},
                timeout=120,
            )
        )

        # Wait for the pause to actually trigger before doing the concurrent INSERT.
        wait_future.result(timeout=60)

        # 3. Concurrent INSERT into the SAME partition (a=1). It commits a new snapshot
        #    that the DROP retry will observe, but the new file is *not* in the locked
        #    target set and must survive.
        instance.query(
            f"INSERT INTO {table_name} VALUES (1, 'inserted-during-drop')",
            settings={"allow_insert_into_iceberg": 1},
        )

        # 4. Release the failpoint. DROP attempt 0's commit fails (parent snapshot moved),
        #    the retry refreshes state and commits successfully.
        instance.query("SYSTEM DISABLE FAILPOINT iceberg_drop_partition_pause_after_discovery")

        # 5. DROP completes.
        drop_future.result(timeout=120)
    finally:
        # Always disable the failpoint -- if anything above raised before the
        # explicit DISABLE ran (e.g. the WAIT timed out), it would otherwise
        # stay enabled and poison subsequent tests on the same instance.
        try:
            instance.query("SYSTEM DISABLE FAILPOINT iceberg_drop_partition_pause_after_discovery")
        except Exception:
            pass
        executor.shutdown(wait=False)

    # 6. The two pre-drop files for a=1 are gone; the concurrently-inserted row survives,
    #    as does the unrelated partition a=2.
    result = instance.query(f"SELECT a, b FROM {table_name} ORDER BY a, b").strip()
    assert result == "1\tinserted-during-drop\n2\tkeep", result
