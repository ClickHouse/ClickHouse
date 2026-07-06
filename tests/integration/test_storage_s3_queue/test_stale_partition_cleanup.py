import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    put_s3_file_content,
    create_table,
    create_mv,
    generate_random_string,
)

# Filenames look like `server-a_20251217T100000.000000Z_0000.csv`; the `hostname`
# capture group is the partition key.
PARTITION_REGEX = (
    r"(?P<hostname>[^_]+)_(?P<timestamp>\d{8}T\d{6}\.\d{6}Z)_(?P<sequence>\d+)"
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
            ],
            user_configs=["configs/users.xml"],
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        yield cluster
    finally:
        cluster.shutdown()


def run_with_retry(check_result, func, retries=120):
    for _ in range(retries):
        last = func()
        if check_result(last):
            return last
        time.sleep(1)
    raise RuntimeError(f"{last} did not match expectations in {retries} retries")


def _put_host_file(started_cluster, files_path, host, i):
    """Upload one file for `host` with a strictly increasing name (one row)."""
    ts = f"20251217T1000{i:02d}.000000Z"
    seq = f"{i:04d}"
    put_s3_file_content(
        started_cluster,
        f"{files_path}/{host}_{ts}_{seq}.csv",
        f"{i},{i},{i}\n".encode(),
    )


def _partition_present(node, keeper_path, host):
    """True if a `buckets/<b>/processed/<host>` watermark node exists in Keeper."""
    buckets = (
        node.query(f"SELECT name FROM system.zookeeper WHERE path = '{keeper_path}/buckets'")
        .strip()
        .split()
    )
    for bucket in buckets:
        children = (
            node.query(f"SELECT name FROM system.zookeeper WHERE path = '{keeper_path}/buckets/{bucket}'")
            .strip()
            .split()
        )
        if "processed" not in children:
            continue
        partitions = (
            node.query(f"SELECT name FROM system.zookeeper WHERE path = '{keeper_path}/buckets/{bucket}/processed'")
            .strip()
            .split()
        )
        if host in partitions:
            return True
    return False


def _partitioned_settings(keeper_path, ttl_sec):
    return {
        "keeper_path": keeper_path,
        "bucketing_mode": "partition",
        "buckets": 4,
        "s3queue_processing_threads_num": 4,
        "cleanup_stale_partitions": 1,
        "stale_partition_ttl_sec": ttl_sec,
    }


def test_stale_partition_removed_and_live_kept(started_cluster):
    """A stale partition whose objects are all gone (fully processed) has its watermark
    node removed; a stale partition that still has an object in storage is kept."""
    node = started_cluster.instances["instance"]

    table_name = f"stale_cleanup_{generate_random_string()}"
    mv_name = f"{table_name}_mv"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    host_drained = "server-a"
    host_live = "server-b"

    create_table(
        started_cluster, node, table_name, "ordered", files_path,
        partitioning_mode="regex", partition_regex=PARTITION_REGEX, partition_component="hostname",
        after_processing="delete",
        additional_settings=_partitioned_settings(keeper_path, ttl_sec=1),
    )
    create_mv(node, table_name, dst_table_name)

    # Process 3 files for each host: watermark nodes get created, files get deleted (drained).
    for i in range(3):
        _put_host_file(started_cluster, files_path, host_drained, i)
        _put_host_file(started_cluster, files_path, host_live, i)

    run_with_retry(lambda x: x == 6, lambda: int(node.query(f"SELECT count() FROM {dst_table_name}")))

    assert _partition_present(node, keeper_path, host_drained)
    assert _partition_present(node, keeper_path, host_live)

    # Stop processing so the next uploaded file is NOT consumed.
    node.query(f"DROP TABLE {mv_name}")

    # Give `host_live` a remaining object in storage; `host_drained` stays empty.
    _put_host_file(started_cluster, files_path, host_live, 9)

    # Let both watermark nodes age past the (1s) TTL. The extra margin also clears the
    # second-granularity of the server clock used for the staleness comparison.
    time.sleep(3)

    # Trigger a cleanup pass deterministically: re-attach re-runs startup(), which schedules
    # the cleanup task immediately. The MV is gone, so nothing gets processed meanwhile.
    node.query(f"DETACH TABLE {table_name}")
    node.query(f"ATTACH TABLE {table_name}")

    # host_drained: stale + no objects left -> node removed.
    run_with_retry(
        lambda removed: removed,
        lambda: not _partition_present(node, keeper_path, host_drained),
    )
    # host_live: stale but still has an object -> node kept.
    assert _partition_present(node, keeper_path, host_live), (
        "watermark node of a partition that still has objects in storage must not be removed"
    )

    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


@pytest.mark.parametrize(
    "case,mode,partitioning,after_processing,expected",
    [
        ("supported_delete", "ordered", "regex", "delete", "true"),
        ("supported_keep", "ordered", "regex", "keep", "true"),
        ("no_partition_not_supported", "ordered", "none", "delete", "false"),
        ("unordered_not_supported", "unordered", "none", "delete", "false"),
    ],
)
def test_stale_cleanup_gating(started_cluster, case, mode, partitioning, after_processing, expected):
    """`cleanup_stale_partitions` is only effective for ordered + regex partitioning +
    after_processing in (delete, move); otherwise it is reported as disabled."""
    node = started_cluster.instances["instance"]

    table_name = f"stale_gating_{case}_{generate_random_string()}"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    additional_settings = {
        "keeper_path": keeper_path,
        "cleanup_stale_partitions": 1,
        "stale_partition_ttl_sec": 3600,
    }
    kwargs = {"after_processing": after_processing, "additional_settings": additional_settings}
    if partitioning == "regex":
        kwargs.update(
            partitioning_mode="regex",
            partition_regex=PARTITION_REGEX,
            partition_component="hostname",
        )
        additional_settings["bucketing_mode"] = "partition"
        additional_settings["buckets"] = 4

    create_table(started_cluster, node, table_name, mode, files_path, **kwargs)

    value = node.query(
        f"SELECT value FROM system.s3_queue_settings "
        f"WHERE table = '{table_name}' AND name = 'cleanup_stale_partitions'"
    ).strip()
    assert value == expected, f"case={case}: expected cleanup_stale_partitions={expected}, got {value}"

    node.query(f"DROP TABLE {table_name}")
