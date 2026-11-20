import io
import json
import logging
import random
import string
import time
import uuid
from multiprocessing.dummy import Pool
from datetime import datetime
import pytest
from kazoo.exceptions import NoNodeError

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.s3_queue_common import (
    generate_random_files,
    create_table,
    create_mv,
)
from helpers.config_cluster import minio_secret_key


@pytest.fixture(autouse=True)
def s3_queue_setup_teardown(started_cluster):
    instance = started_cluster.instances["instance_24.5"]
    instance_2 = started_cluster.instances["instance2_24.5"]

    instance.query("DROP DATABASE IF EXISTS default; CREATE DATABASE default;")
    instance_2.query("DROP DATABASE IF EXISTS default; CREATE DATABASE default;")

    minio = started_cluster.minio_client
    objects = list(minio.list_objects(started_cluster.minio_bucket, recursive=True))
    for obj in objects:
        minio.remove_object(started_cluster.minio_bucket, obj.object_name)

    yield  # run test


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance_24.5",
            with_zookeeper=True,
            with_minio=True,
            image="clickhouse/clickhouse-server",
            tag="24.5",
            stay_alive=True,
            user_configs=[
                "configs/users.xml",
                "configs/compatibility.xml",
            ],
            main_configs=[
                "configs/s3queue_log.xml",
                "configs/remote_servers_245.xml",
            ],
            with_installed_binary=True,
        )
        cluster.add_instance(
            "instance2_24.5",
            with_zookeeper=True,
            with_minio=True,
            keeper_required_feature_flags=["create_if_not_exists"],
            image="clickhouse/clickhouse-server",
            tag="24.5",
            stay_alive=True,
            user_configs=[
                "configs/users.xml",
                "configs/compatibility.xml",
            ],
            main_configs=[
                "configs/s3queue_log.xml",
                "configs/remote_servers_245.xml",
            ],
            with_installed_binary=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.not_repeatable
@pytest.mark.parametrize("setting_prefix", ["", "s3queue_"])
@pytest.mark.parametrize("buckets_num", [3, 1])
def test_migration(started_cluster, setting_prefix, buckets_num):
    node1 = started_cluster.instances["instance_24.5"]
    node2 = started_cluster.instances["instance2_24.5"]

    for node in [node1, node2]:
        if "24.5" not in node.query("select version()").strip():
            node.restart_with_original_version()

    table_name = f"test_replicated_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    mv_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}_{buckets_num}"
    files_path = f"{table_name}_data"

    for node in [node1, node2]:
        node.query("DROP DATABASE IF EXISTS r SYNC")

    # Clean up the ZK path in case DROP DATABASE on an older binary didn't fully remove it
    zk_client = started_cluster.get_kazoo_client("zoo1")
    if zk_client.exists("/clickhouse/databases/replicateddb3"):
        zk_client.delete("/clickhouse/databases/replicateddb3", recursive=True)

    node1.query(
        "CREATE DATABASE r ENGINE=Replicated('/clickhouse/databases/replicateddb3', 'shard1', 'node1')"
    )
    node2.query(
        "CREATE DATABASE r ENGINE=Replicated('/clickhouse/databases/replicateddb3', 'shard1', 'node2')"
    )

    create_table(
        started_cluster,
        node1,
        table_name,
        "ordered",
        files_path,
        version="24.5",
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_polling_min_timeout_ms": 100,
            "s3queue_polling_max_timeout_ms": 1000,
            "s3queue_polling_backoff_ms": 100,
        },
        database_name="r",
    )

    for node in [node1, node2]:
        create_mv(node, f"r.{table_name}", dst_table_name, mv_name=mv_name)

    start_ind = [0]
    expected_rows = [0]
    last_processed_path = [""]
    prefix_ind = [0]
    prefixes = ["a", "b", "c", "d", "e"]

    def add_files_and_check():
        rows = 1000
        use_prefix = prefixes[prefix_ind[0]]
        total_values = generate_random_files(
            started_cluster,
            files_path,
            rows,
            start_ind=start_ind[0],
            row_num=1,
            use_prefix=use_prefix,
        )
        expected_rows[0] += rows
        start_ind[0] += rows
        prefix_ind[0] += 1

        def get_count():
            return int(
                node1.query(
                    f"SELECT count() FROM clusterAllReplicas(cluster, default.{dst_table_name})"
                )
            )

        last_processed_path[0] = f"{use_prefix}_{expected_rows[0] - 1}.csv"
        for _ in range(50):
            if expected_rows[0] == get_count():
                break
            time.sleep(1)

        if expected_rows[0] != get_count():
            files_to_generate = expected_rows[0]  # 1 row per file
            expected_files = [
                f"{files_path}/test_{x}.csv" for x in range(files_to_generate)
            ]

            for node in [node1, node2]:
                node.query("SYSTEM FLUSH LOGS")

            processed_files = (
                node.query(
                    f"SELECT distinct(_path) FROM clusterAllReplicas(cluster, default.{dst_table_name})"
                )
                .strip()
                .split("\n")
            )

            processed_files.sort()
            logging.debug(f"Processed files: {processed_files}")
            missing_files = [
                file for file in expected_files if file not in processed_files
            ]
            missing_files.sort()

            assert (
                False
            ), f"Expected {total_rows} in total, got {count1} and {count2} ({count1 + count2}, having {len(missing_files)} missing files: ({missing_files})"

    add_files_and_check()

    zk = started_cluster.get_kazoo_client("zoo1")
    metadata = json.loads(zk.get(f"{keeper_path}/processed")[0])

    assert last_processed_path[0].startswith("a_")
    assert metadata["file_path"].endswith(last_processed_path[0])

    for node in [node1, node2]:
        node.restart_with_latest_version()
        assert 0 == int(
            node.query(
                f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'buckets'"
            )
        )

    assert (
        "Changing setting buckets is not allowed only with detached dependencies"
        in node1.query_and_get_error(
            f"ALTER TABLE r.{table_name} MODIFY SETTING {setting_prefix}buckets={buckets_num}"
        )
    )

    for node in [node1, node2]:
        node.query(f"DETACH TABLE {mv_name} SYNC")

    assert (
        "To allow migration set s3queue_migrate_old_metadata_to_buckets = 1"
        in node1.query_and_get_error(
            f"ALTER TABLE r.{table_name} MODIFY SETTING {setting_prefix}buckets={buckets_num}"
        )
    )

    def migrate_to_buckets(value):
        node1.query(
            f"ALTER TABLE r.{table_name} MODIFY SETTING {setting_prefix}buckets={value} SETTINGS s3queue_migrate_old_metadata_to_buckets = 1"
        )

    def check_keeper_state_changed():
        for node in [node1, node2]:
            assert buckets_num == int(
                node.query(
                    f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'buckets'"
                )
            )

        metadata = json.loads(zk.get(f"{keeper_path}/metadata/")[0])
        assert buckets_num == metadata["buckets"]

        try:
            zk.get(f"{keeper_path}/processed")
            assert False
        except NoNodeError:
            pass

        buckets = zk.get_children(f"{keeper_path}/buckets/")

        assert len(buckets) == buckets_num
        assert sorted(buckets) == [str(i) for i in range(buckets_num)]

        for i in range(buckets_num):
            path = f"{keeper_path}/buckets/{i}/processed"
            print(f"Checking {path}")
            metadata = json.loads(zk.get(path)[0])
            assert metadata["file_path"].endswith(last_processed_path[0])

    migrate_to_buckets(buckets_num)
    check_keeper_state_changed()

    if buckets_num == 1:
        correct_value = 3
        migrate_to_buckets(correct_value)
        buckets_num = correct_value
        check_keeper_state_changed()

    for node in [node1, node2]:
        node.query(f"ATTACH TABLE {mv_name}")

    add_files_and_check()

    for node in [node1, node2]:
        node.restart_clickhouse()
        assert buckets_num == int(
            node.query(
                f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'buckets'"
            )
        )

    add_files_and_check()

    try:
        zk.get(f"{keeper_path}/processed")
        assert False
    except NoNodeError:
        pass

    buckets = zk.get_children(f"{keeper_path}/buckets/")
    assert len(buckets) == buckets_num

    found = False
    for i in range(buckets_num):
        metadata = json.loads(zk.get(f"{keeper_path}/buckets/{i}/processed")[0])
        if metadata["file_path"].endswith(last_processed_path[0]):
            found = True
            break
    assert found

    metadata = json.loads(zk.get(f"{keeper_path}/metadata/")[0])
    assert buckets_num == metadata["buckets"]

    node.query(f"DROP TABLE r.{table_name} SYNC")

    for node in [node1, node2]:
        node.query("DROP DATABASE IF EXISTS r SYNC")
