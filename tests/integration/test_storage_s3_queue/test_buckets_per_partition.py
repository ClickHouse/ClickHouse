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

# Filenames look like `server-1_20251217T100000.000000Z_0000.csv`; the `hostname`
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


def run_with_retry(check_result, func, retries=300):
    for _ in range(retries):
        last = func()
        if check_result(last):
            return last
        time.sleep(1)
    raise RuntimeError(f"{last} did not match expectations in {retries} retries")


def _put_partition_files(started_cluster, files_path, hostname, count):
    """Put `count` files for one hostname with strictly increasing filenames."""
    for i in range(count):
        # Both timestamp and sequence increase with i, so the filenames of a single
        # hostname form a strictly increasing (lexicographic == numeric) sequence.
        ts = f"20251217T1000{i:02d}.000000Z"
        seq = f"{i:04d}"
        put_s3_file_content(
            started_cluster,
            f"{files_path}/{hostname}_{ts}_{seq}.csv",
            f"{i},{i},{i}\n".encode(),
        )


def _buckets_used_by_partition(node, keeper_path, hostname):
    """Number of buckets whose `processed/<hostname>` watermark node exists."""
    buckets = (
        node.query(f"SELECT name FROM system.zookeeper WHERE path = '{keeper_path}/buckets'")
        .strip()
        .split()
    )
    used = 0
    for bucket in buckets:
        children = (
            node.query(
                f"SELECT name FROM system.zookeeper WHERE path = '{keeper_path}/buckets/{bucket}'"
            )
            .strip()
            .split()
        )
        if "processed" not in children:
            continue
        partitions = (
            node.query(
                f"SELECT name FROM system.zookeeper WHERE path = '{keeper_path}/buckets/{bucket}/processed'"
            )
            .strip()
            .split()
        )
        if hostname in partitions:
            used += 1
    return used


def _partitioned_settings(keeper_path, buckets, buckets_per_partition):
    return {
        "keeper_path": keeper_path,
        "bucketing_mode": "partition",
        "buckets": buckets,
        "buckets_per_partition": buckets_per_partition,
        "s3queue_processing_threads_num": buckets,
    }


def test_buckets_per_partition_validation(started_cluster):
    """`buckets_per_partition` is validated at table creation."""
    node = started_cluster.instances["instance"]
    base = f"bpp_val_{generate_random_string()}"

    # Zero is not allowed.
    err = create_table(
        started_cluster, node, f"{base}_zero", "ordered", f"{base}_zero_data",
        additional_settings={"keeper_path": f"/clickhouse/{base}_zero", "buckets_per_partition": 0},
        expect_error=True,
    )
    assert "buckets_per_partition" in err

    # > 1 requires bucketing_mode='partition'.
    err = create_table(
        started_cluster, node, f"{base}_mode", "ordered", f"{base}_mode_data",
        additional_settings={"keeper_path": f"/clickhouse/{base}_mode", "buckets": 8, "buckets_per_partition": 4},
        expect_error=True,
    )
    assert "bucketing_mode" in err

    # Cannot exceed the number of buckets.
    err = create_table(
        started_cluster, node, f"{base}_toobig", "ordered", f"{base}_toobig_data",
        partitioning_mode="regex", partition_regex=PARTITION_REGEX, partition_component="hostname",
        additional_settings={
            "keeper_path": f"/clickhouse/{base}_toobig",
            "bucketing_mode": "partition", "buckets": 4, "buckets_per_partition": 8,
        },
        expect_error=True,
    )
    assert "cannot be greater than" in err


@pytest.mark.parametrize("buckets_per_partition", [1, 4])
def test_buckets_per_partition_spread(started_cluster, buckets_per_partition):
    """A single partition's files are spread across `buckets_per_partition` buckets,
    and every file is still processed exactly once (per-partition ordering preserved)."""
    node = started_cluster.instances["instance"]
    table_name = f"bpp_spread_{buckets_per_partition}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    hostname = "server-1"
    num_files = 24

    create_table(
        started_cluster, node, table_name, "ordered", files_path,
        partitioning_mode="regex", partition_regex=PARTITION_REGEX, partition_component="hostname",
        additional_settings=_partitioned_settings(keeper_path, 8, buckets_per_partition),
    )
    _put_partition_files(started_cluster, files_path, hostname, num_files)
    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    # All files processed exactly once: nothing skipped (monotonicity) and nothing duplicated.
    run_with_retry(lambda x: x == num_files, get_count)
    assert get_count() == num_files

    used = _buckets_used_by_partition(node, keeper_path, hostname)
    if buckets_per_partition == 1:
        assert used == 1, f"Expected the partition in exactly 1 bucket, got {used}"
    else:
        assert used > 1, (
            f"Expected the partition spread across >1 bucket with "
            f"buckets_per_partition={buckets_per_partition}, got {used}"
        )

    node.query(f"DROP TABLE {dst_table_name}; DROP TABLE {table_name};")


def test_buckets_per_partition_immutable(started_cluster):
    """`buckets_per_partition` is fixed at table creation: a second table sharing the
    same keeper_path but a different value is rejected."""
    node = started_cluster.instances["instance"]
    base = f"bpp_immut_{generate_random_string()}"
    keeper_path = f"/clickhouse/test_{base}"

    create_table(
        started_cluster, node, f"{base}_a", "ordered", f"{base}_data",
        partitioning_mode="regex", partition_regex=PARTITION_REGEX, partition_component="hostname",
        additional_settings=_partitioned_settings(keeper_path, 8, 4),
    )

    err = create_table(
        started_cluster, node, f"{base}_b", "ordered", f"{base}_data",
        partitioning_mode="regex", partition_regex=PARTITION_REGEX, partition_component="hostname",
        additional_settings=_partitioned_settings(keeper_path, 8, 2),
        expect_error=True,
    )
    assert "buckets_per_partition" in err

    node.query(f"DROP TABLE {base}_a")


def test_buckets_per_partition_reported_and_persisted(started_cluster):
    """The value is reported by system.s3_queue_settings and survives a restart
    (i.e. it is serialized to and parsed back from keeper)."""
    node = started_cluster.instances["instance"]
    table_name = f"bpp_persist_{generate_random_string()}"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    create_table(
        started_cluster, node, table_name, "ordered", files_path,
        partitioning_mode="regex", partition_regex=PARTITION_REGEX, partition_component="hostname",
        additional_settings=_partitioned_settings(keeper_path, 8, 4),
    )

    def get_setting():
        return node.query(
            f"SELECT value FROM system.s3_queue_settings "
            f"WHERE table = '{table_name}' AND name = 'buckets_per_partition'"
        ).strip()

    assert get_setting() == "4"

    node.restart_clickhouse()
    run_with_retry(lambda x: x == "4", get_setting)

    node.query(f"DROP TABLE {table_name}")
