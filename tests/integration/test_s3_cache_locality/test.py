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


def create_buckets_s3(cluster, files=1000):
    minio = cluster.minio_client

    s3_data = []

    for file_number in range(files):
        file_name = f"data/generated_{files}/file_{file_number}.csv"
        os.makedirs(os.path.join(SCRIPT_DIR, f"data/generated_{files}/"), exist_ok=True)
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
                stay_alive=True,
            )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        create_buckets_s3(cluster)
        create_buckets_s3(cluster, files=3)

        yield cluster
    finally:
        shutil.rmtree(os.path.join(SCRIPT_DIR, "data/generated_1000/"), ignore_errors=True)
        shutil.rmtree(os.path.join(SCRIPT_DIR, "data/generated_3/"), ignore_errors=True)
        cluster.shutdown()


def check_s3_gets(cluster, node, expected_result, cluster_first, cluster_second, enable_filesystem_cache,
                  lock_object_storage_task_distribution_ms, files=1000):
    for host in list(cluster.instances.values()):
        host.query("SYSTEM DROP FILESYSTEM CACHE 'raw_s3_cache'", ignore_error=True)

    settings = {
        "enable_filesystem_cache": enable_filesystem_cache,
        "filesystem_cache_name": "'raw_s3_cache'",
        }

    settings["lock_object_storage_task_distribution_ms"] = lock_object_storage_task_distribution_ms

    query_id_first = str(uuid.uuid4())
    result_first = node.query(
        f"""
        SELECT count(*)
          FROM s3Cluster('{cluster_first}', 'http://minio1:9001/root/data/generated_{files}/*', 'minio', '{minio_secret_key}', 'CSV', 'a String, b UInt64')
          WHERE b=42
        SETTINGS {",".join(f"{k}={v}" for k, v in settings.items())}
        """,
        query_id=query_id_first,
    )
    assert result_first == expected_result
    query_id_second = str(uuid.uuid4())
    result_second = node.query(
        f"""
        SELECT count(*)
          FROM s3Cluster('{cluster_second}', 'http://minio1:9001/root/data/generated_{files}/*', 'minio', '{minio_secret_key}', 'CSV', 'a String, b UInt64')
          WHERE b=42
        SETTINGS {",".join(f"{k}={v}" for k, v in settings.items())}
        """,
        query_id=query_id_second,
    )
    assert result_second == expected_result

    node.query(f"SYSTEM FLUSH LOGS ON CLUSTER {cluster_first}")
    node.query(f"SYSTEM FLUSH LOGS ON CLUSTER {cluster_second}")

    s3_get_first = node.query(
        f"""
        SELECT sum(ProfileEvents['S3GetObject'])
          FROM clusterAllReplicas('{cluster_first}', system.query_log)
          WHERE type='QueryFinish'
            AND initial_query_id='{query_id_first}'
        """,
    )
    s3_get_second = node.query(
        f"""
        SELECT sum(ProfileEvents['S3GetObject'])
          FROM clusterAllReplicas('{cluster_second}', system.query_log)
          WHERE type='QueryFinish'
            AND initial_query_id='{query_id_second}'
        """,
    )

    return int(s3_get_first), int(s3_get_second)


def check_s3_gets_by_hosts(cluster, node, expected_result,
                  lock_object_storage_task_distribution_ms, files=1000):
    settings = {
        "enable_filesystem_cache": False,
        }

    settings["lock_object_storage_task_distribution_ms"] = lock_object_storage_task_distribution_ms
    query_id = str(uuid.uuid4())
    result = node.query(
        f"""
        SELECT count(*)
          FROM s3Cluster('{cluster}', 'http://minio1:9001/root/data/generated_{files}/*', 'minio', '{minio_secret_key}', 'CSV', 'a String, b UInt64')
          WHERE b=42
        SETTINGS {",".join(f"{k}={v}" for k, v in settings.items())}
        """,
        query_id=query_id,
    )
    assert result == expected_result

    node.query(f"SYSTEM FLUSH LOGS ON CLUSTER {cluster}")

    s3_get = node.query(
        f"""
        SELECT ProfileEvents['S3GetObject']
          FROM clusterAllReplicas('{cluster}', system.query_log)
          WHERE type='QueryFinish'
            AND initial_query_id='{query_id}'
          ORDER BY hostname
        """,
    )

    return [int(events) for events in s3_get.strip().split("\n")]


def check_s3_gets_repeat(cluster, node, expected_result, cluster_first, cluster_second, enable_filesystem_cache,
                         lock_object_storage_task_distribution_ms):
    # Repeat test several times to get average result
    iterations = 1 if lock_object_storage_task_distribution_ms > 0 else 10
    s3_get_first_sum = 0
    s3_get_second_sum = 0
    for _ in range(iterations):
        (s3_get_first, s3_get_second) = check_s3_gets(cluster, node, expected_result, cluster_first, cluster_second, enable_filesystem_cache, lock_object_storage_task_distribution_ms)
        s3_get_first_sum += s3_get_first
        s3_get_second_sum += s3_get_second
    return s3_get_first_sum, s3_get_second_sum


@pytest.mark.parametrize("lock_object_storage_task_distribution_ms ", [0, 30000])
def test_cache_locality(started_cluster, lock_object_storage_task_distribution_ms):
    node = started_cluster.instances["clickhouse0"]

    expected_result = node.query(
        f"""
        SELECT count(*)
          FROM s3('http://minio1:9001/root/data/generated_1000/*', 'minio', '{minio_secret_key}', 'CSV', 'a String, b UInt64')
          WHERE b=42
        """
    )

    # Algorithm does not give 100% guarantee, so add 10% on dispersion
    dispersion = 0.0 if lock_object_storage_task_distribution_ms > 0 else 0.1

    # No cache
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_12345', 0, lock_object_storage_task_distribution_ms)
    assert s3_get_second == s3_get_first

    # With cache
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_12345', 1, lock_object_storage_task_distribution_ms)
    assert s3_get_second <= s3_get_first * dispersion

    # Different replicas order
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_34512', 1, lock_object_storage_task_distribution_ms)
    assert s3_get_second <= s3_get_first * dispersion

    # No last replica
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_1234', 1, lock_object_storage_task_distribution_ms)
    assert s3_get_second <= s3_get_first * (0.179 + dispersion) # actual value - 179 of 1000 files changed replica

    # No first replica
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_2345', 1, lock_object_storage_task_distribution_ms)
    assert s3_get_second <= s3_get_first * (0.189 + dispersion) # actual value - 189 of 1000 files changed replica

    # No first replica, different replicas order
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_12345', 'cluster_4523', 1, lock_object_storage_task_distribution_ms)
    assert s3_get_second <= s3_get_first * (0.189 + dispersion)

    # Add new replica, different replicas order
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_4523', 'cluster_12345', 1, lock_object_storage_task_distribution_ms)
    assert s3_get_second <= s3_get_first * (0.189 + dispersion)

    # New replica and old replica, different replicas order
    # All files from removed replica changed replica
    # Some files from existed replicas changed replica on the new replica
    (s3_get_first, s3_get_second) = check_s3_gets_repeat(started_cluster, node, expected_result, 'cluster_1234', 'cluster_4523', 1, lock_object_storage_task_distribution_ms)
    assert s3_get_second <= s3_get_first * (0.368 + dispersion) # actual value - 368 of 1000 changed replica

    if (lock_object_storage_task_distribution_ms > 0):
        s3_get = check_s3_gets_by_hosts('cluster_12345', node, expected_result, lock_object_storage_task_distribution_ms, files=1000)
        assert s3_get == [189,210,220,202,179]
        s3_get = check_s3_gets_by_hosts('cluster_1234', node, expected_result, lock_object_storage_task_distribution_ms, files=1000)
        assert s3_get == [247,243,264,246]
        s3_get = check_s3_gets_by_hosts('cluster_2345', node, expected_result, lock_object_storage_task_distribution_ms, files=1000)
        assert s3_get == [251,280,248,221]


def test_cache_locality_few_files(started_cluster):
    node = started_cluster.instances["clickhouse0"]

    expected_result = node.query(
        f"""
        SELECT count(*)
          FROM s3('http://minio1:9001/root/data/generated_3/*', 'minio', '{minio_secret_key}', 'CSV', 'a String, b UInt64')
          WHERE b=42
        """
    )

    # Rendezvous hash makes the next distribution:
    # file_0 - clickhouse1
    # file_1 - clickhouse4
    # file_2 - clickhouse3
    # The same distribution must be in each query
    for _ in range(10):
        s3_get = check_s3_gets_by_hosts('cluster_12345', node, expected_result, lock_object_storage_task_distribution_ms=30000, files=3)
        assert s3_get == [1,0,1,1,0]
