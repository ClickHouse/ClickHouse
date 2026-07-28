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
