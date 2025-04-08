import csv
import logging
import os
import shutil
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def create_buckets_s3(cluster):
    minio = cluster.minio_client

    s3_data = []

    for file_number in range(1000):
        file_name = f"data/generated/file_{file_number}.csv"
        os.makedirs(os.path.join(SCRIPT_DIR, "data/generated/"), exist_ok=True)
        s3_data.append(file_name)
        with open(os.path.join(SCRIPT_DIR, file_name), "w+", encoding="utf-8") as f:
            # a String, b UInt64
            data = []

            # Make all files a bit different
            data.append(
                ["str_" + str(file_number), file_number]
            )

            writer = csv.writer(f)
            writer.writerows(data)

    for file in s3_data:
        minio.fput_object(
            bucket_name=cluster.minio_bucket,
            object_name=file,
            file_path=os.path.join(SCRIPT_DIR, file),
        )

    for obj in minio.list_objects(cluster.minio_bucket, recursive=True):
        print(obj.object_name)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        # clickhouse0 not a member of cluster_XXX
        for i in range(6):
            cluster.add_instance(
                f"clickhouse{i}",
                main_configs=["configs/cluster.xml", "configs/named_collections.xml"],
                user_configs=["configs/users.xml"],
                macros={"replica": f"clickhouse{i}"},
                with_minio=True,
                with_zookeeper=True,
            )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        create_buckets_s3(cluster)

        yield cluster
    finally:
        shutil.rmtree(os.path.join(SCRIPT_DIR, "data/generated/"))
        cluster.shutdown()


def check_s3_gets(cluster, node, expected_result, cluster_first, cluster_second, enable_filesystem_cache):
    for host in list(cluster.instances.values()):
        host.query("SYSTEM DROP FILESYSTEM CACHE 'raw_s3_cache'")

    query_id_first = str(uuid.uuid4())
    result_first = node.query(
        f"""
        SELECT count(*)
          FROM s3Cluster('{cluster_first}', 'http://minio1:9001/root/data/generated/*', 'minio', '{minio_secret_key}', 'CSV', 'a String, b UInt64')
          WHERE b=42
        SETTINGS
          enable_filesystem_cache={enable_filesystem_cache},
          filesystem_cache_name='raw_s3_cache'
        """,
        query_id=query_id_first
    )
    assert result_first == expected_result
    query_id_second = str(uuid.uuid4())
    result_second = node.query(
        f"""
        SELECT count(*)
          FROM s3Cluster('{cluster_second}', 'http://minio1:9001/root/data/generated/*', 'minio', '{minio_secret_key}', 'CSV', 'a String, b UInt64')
          WHERE b=42
        SETTINGS
          enable_filesystem_cache={enable_filesystem_cache},
          filesystem_cache_name='raw_s3_cache'
        """,
        query_id=query_id_second
    )
    assert result_second == expected_result

    node.query("SYSTEM FLUSH LOGS")
    node.query(f"SYSTEM FLUSH LOGS ON CLUSTER {cluster_first}")
    node.query(f"SYSTEM FLUSH LOGS ON CLUSTER {cluster_second}")

    s3_get_first = node.query(
        f"""
        SELECT sum(ProfileEvents['S3GetObject'])
          FROM clusterAllReplicas('{cluster_first}', system.query_log)
          WHERE type='QueryFinish'
            AND initial_query_id='{query_id_first}'
        """
    )
    s3_get_second = node.query(
        f"""
        SELECT sum(ProfileEvents['S3GetObject'])
          FROM clusterAllReplicas('{cluster_second}', system.query_log)
          WHERE type='QueryFinish'
            AND initial_query_id='{query_id_second}'
        """
    )

    return int(s3_get_first), int(s3_get_second)


def check_s3_gets_repeat(cluster, node, expected_result, cluster_first, cluster_second, enable_filesystem_cache):
    # Repeat test several times to get average result
    iterations = 10
    s3_get_first_sum = 0
    s3_get_second_sum = 0
    for _ in range(iterations):
        (s3_get_first, s3_get_second) = check_s3_gets(cluster, node, expected_result, cluster_first, cluster_second, enable_filesystem_cache)
        s3_get_first_sum += s3_get_first
        s3_get_second_sum += s3_get_second
    return s3_get_first_sum, s3_get_second_sum


def test_cache_locality(started_cluster):
    node = started_cluster.instances["clickhouse0"]

    expected_result = node.query(
        f"""
        SELECT count(*)
          FROM s3('http://minio1:9001/root/data/generated/*', 'minio', '{minio_secret_key}', 'CSV', 'a String, b UInt64')
          WHERE b=42
        """
    )

    # Algorithm does not give 100% guarantee, so add 10% on dispersion
    dispersion = 0.1

    # No cache
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_12345', 0)
    assert s3_get_second == s3_get_first

    # With cache
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_12345', 1)
    assert s3_get_second <= s3_get_first * dispersion

    # Different nodes order
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_34512', 1)
    assert s3_get_second <= s3_get_first * dispersion

    # No last node
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_1234', 1)
    assert s3_get_second <= s3_get_first * (0.2 + dispersion)

    # No first node
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_2345', 1)
    assert s3_get_second <= s3_get_first * (0.2 + dispersion)

    # No first node, different nodes order
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_4523', 1)
    assert s3_get_second <= s3_get_first * (0.2 + dispersion)

    # Add new node, different nodes order
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_4523', 'cluster_12345', 1)
    assert s3_get_second <= s3_get_first * (0.2 + dispersion)

    # New node and old node, different nodes order
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_1234', 'cluster_4523', 1)
    assert s3_get_second <= s3_get_first * (0.4375 + dispersion)
