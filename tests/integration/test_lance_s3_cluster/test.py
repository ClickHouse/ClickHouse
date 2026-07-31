import io
import json
import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def upload_lance_dataset_to_minio(started_cluster, remote_prefix, dataset_name="multi_frag.lance"):
    local_path = os.path.abspath(
        os.path.join(
            SCRIPT_DIR,
            "../../queries/0_stateless/data_lance",
            dataset_name,
        )
    )
    for root, _, files in os.walk(local_path):
        for filename in files:
            local_file = os.path.join(root, filename)
            relative_path = os.path.relpath(local_file, local_path)
            remote_path = os.path.join(remote_prefix, relative_path)
            started_cluster.minio_client.fput_object(
                bucket_name=started_cluster.minio_bucket,
                object_name=remote_path,
                file_path=local_file,
            )


def skip_if_lance_s3_cluster_unavailable(instance):
    if (
        instance.query(
            "SELECT count() FROM system.table_functions WHERE name = 'lanceS3Cluster'"
        )
        == "0\n"
    ):
        pytest.skip("lanceS3Cluster table function is not available in this build")


@pytest.fixture(scope="module")
def started_cluster():
    cluster = None
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "s0_0_0",
            main_configs=["configs/cluster.xml", "configs/named_collections.xml"],
            user_configs=["configs/users.xml"],
            macros={"replica": "node1", "shard": "shard1"},
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "s0_0_1",
            main_configs=["configs/cluster.xml", "configs/named_collections.xml"],
            user_configs=["configs/users.xml"],
            macros={"replica": "replica2", "shard": "shard1"},
            with_zookeeper=True,
        )
        cluster.add_instance(
            "s0_1_0",
            main_configs=["configs/cluster.xml", "configs/named_collections.xml"],
            user_configs=["configs/users.xml"],
            macros={"replica": "replica1", "shard": "shard2"},
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        yield cluster
    finally:
        if cluster is not None:
            cluster.shutdown()


def test_lance_s3_cluster_matches_single_node(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    skip_if_lance_s3_cluster_unavailable(node)

    remote_prefix = "data/lance/multi_frag.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    single = node.query(
        """
        SELECT count(), sum(id), sum(cityHash64(name))
        FROM lanceS3(nc_s3, filename = 'lance/multi_frag.lance')
        SETTINGS lance_enable_fragment_parallelism = 0
        """
    )
    clustered = node.query(
        """
        SELECT count(), sum(id), sum(cityHash64(name))
        FROM lanceS3Cluster(
            'cluster_simple',
            nc_s3,
            filename = 'lance/multi_frag.lance')
        SETTINGS
            lance_enable_fragment_parallelism = 1,
            lance_max_fragment_packs = 8,
            max_threads = 4
        """
    )
    assert clustered == single
    assert single.startswith("64\t")

    virtuals = node.query(
        """
        SELECT
            count(),
            uniqExact(_path),
            uniqExact(_file),
            countIf(position(_path, '/pack') > 0)
        FROM lanceS3Cluster(
            'cluster_simple',
            nc_s3,
            filename = 'lance/multi_frag.lance')
        SETTINGS lance_max_fragment_packs = 8
        """
    )
    assert virtuals == "64\t1\t1\t0\n"


def test_lance_s3_cluster_pure_count_uses_fragment_tasks(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    skip_if_lance_s3_cluster_unavailable(node)

    remote_prefix = "data/lance/multi_frag.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    single = node.query(
        """
        SELECT count()
        FROM lanceS3(nc_s3, filename = 'lance/multi_frag.lance')
        """
    )
    clustered = node.query(
        """
        SELECT count()
        FROM lanceS3Cluster(
            'cluster_simple',
            nc_s3,
            filename = 'lance/multi_frag.lance')
        SETTINGS
            lance_enable_fragment_parallelism = 1,
            lance_max_fragment_packs = 8,
            max_threads = 4
        """
    )
    assert clustered == single
    assert clustered == "64\n"


def test_lance_s3_cluster_worker_does_not_fetch_latest_snapshot(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    skip_if_lance_s3_cluster_unavailable(node)

    remote_prefix = "data/lance/multi_frag.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    instances = list(started_cluster.instances.values())
    try:
        for instance in instances:
            instance.query(
                "SYSTEM ENABLE FAILPOINT datalake_distributed_worker_latest_snapshot"
            )

        result = node.query(
            """
            SELECT count(), sum(id)
            FROM lanceS3Cluster(
                'cluster_simple',
                nc_s3,
                filename = 'lance/multi_frag.lance')
            SETTINGS lance_max_fragment_packs = 8
            """
        )
        assert result == "64\t2080\n"
    finally:
        for instance in instances:
            instance.query(
                "SYSTEM DISABLE FAILPOINT datalake_distributed_worker_latest_snapshot"
            )


def test_lance_s3_cluster_filter(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    skip_if_lance_s3_cluster_unavailable(node)

    remote_prefix = "data/lance/multi_frag.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    single = node.query(
        """
        SELECT count(), sum(id)
        FROM lanceS3(nc_s3, filename = 'lance/multi_frag.lance')
        WHERE id <= 10
        """
    )
    clustered = node.query(
        """
        SELECT count(), sum(id)
        FROM lanceS3Cluster(
            'cluster_simple',
            nc_s3,
            filename = 'lance/multi_frag.lance')
        WHERE id <= 10
        SETTINGS lance_max_fragment_packs = 8
        """
    )
    assert clustered == single
    assert single == "10\t55\n"


def test_lance_s3_cluster_bounded_queue_partial_filter_and_limit(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    skip_if_lance_s3_cluster_unavailable(node)

    remote_prefix = "data/lance/multi_frag.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    partial = node.query(
        """
        SELECT count(), sum(id), uniqExact(id)
        FROM lanceS3Cluster(
            'cluster_simple',
            nc_s3,
            filename = 'lance/multi_frag.lance')
        WHERE id <= 20 AND id % 3 = 0
        SETTINGS
            lance_batch_queue_capacity = 1,
            lance_batch_queue_bytes = 1048576,
            lance_max_fragment_packs = 2,
            max_threads = 4
        """
    )
    assert partial == "6\t63\t6\n"

    limited = node.query(
        """
        SELECT count(), uniqExact(id)
        FROM
        (
            SELECT id
            FROM lanceS3Cluster(
                'cluster_simple',
                nc_s3,
                filename = 'lance/multi_frag.lance')
            LIMIT 7
            SETTINGS
                lance_batch_queue_capacity = 1,
                lance_max_fragment_packs = 2,
                max_threads = 4
        )
        """
    )
    assert limited == "7\t7\n"


def test_lance_s3_cluster_task_scanners_are_bounded(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    skip_if_lance_s3_cluster_unavailable(node)

    remote_prefix = "data/lance/multi_frag.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)
    log_comment = "lance_cluster_scanner_cap"

    result = node.query(
        f"""
        SELECT count(), sum(id), uniqExact(id)
        FROM lanceS3Cluster(
            'cluster_simple',
            nc_s3,
            filename = 'lance/multi_frag.lance')
        SETTINGS
            lance_max_fragment_packs = 2,
            max_threads = 8,
            log_comment = '{log_comment}'
        """
    )
    assert result == "64\t2080\t64\n"

    plan_counts = []
    for instance in started_cluster.instances.values():
        instance.query("SYSTEM FLUSH LOGS query_log")
        values = instance.query(
            f"""
            SELECT ProfileEvents['LancePlanScan']
            FROM system.query_log
            WHERE type = 'QueryFinish' AND log_comment = '{log_comment}'
            """
        )
        plan_counts.extend(int(value) for value in values.splitlines())

    assert plan_counts
    assert sum(plan_counts) >= 1
    assert max(plan_counts) <= 2
