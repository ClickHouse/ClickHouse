import io
import json
import logging
import random
import string
import time
import uuid
from multiprocessing.dummy import Pool

import pytest
from kazoo.exceptions import NoNodeError

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.s3_queue_common import (
    run_query,
    random_str,
    generate_random_files,
    put_s3_file_content,
    put_azure_file_content,
    create_table,
    create_mv,
    generate_random_string,
)

AVAILABLE_MODES = ["unordered", "ordered"]


@pytest.fixture(autouse=True)
def s3_queue_setup_teardown(started_cluster):
    instance = started_cluster.instances["instance"]
    instance_2 = started_cluster.instances["instance2"]

    instance.query("DROP DATABASE IF EXISTS default; CREATE DATABASE default;")
    instance_2.query("DROP DATABASE IF EXISTS default; CREATE DATABASE default;")

    minio = started_cluster.minio_client
    objects = list(minio.list_objects(started_cluster.minio_bucket, recursive=True))
    for obj in objects:
        minio.remove_object(started_cluster.minio_bucket, obj.object_name)

    container_client = started_cluster.blob_service_client.get_container_client(
        started_cluster.azurite_container
    )

    if container_client.exists():
        blob_names = [b.name for b in container_client.list_blobs()]
        logging.debug(f"Deleting blobs: {blob_names}")
        for b in blob_names:
            container_client.delete_blob(b)

    yield  # run test


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            with_minio=True,
            with_azurite=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
                "configs/disable_streaming.xml",
            ],
            user_configs=[
                "configs/users.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "instance2",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "instance_24.5",
            with_zookeeper=True,
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


def test_upgrade_3(started_cluster):
    node = started_cluster.instances["instance_24.5"]
    if "24.5" not in node.query("select version()").strip():
        node.restart_with_original_version()
    assert "24.5" in node.query("select version()").strip()

    table_name = f"test_upgrade_3_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 10

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        no_settings=True,
        version="24.5",
    )
    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    expected_rows = 10
    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)

    assert expected_rows == get_count()

    node.restart_with_latest_version()

    assert table_name in node.query("SHOW TABLES")

    node.query(
        f"""
        ALTER TABLE {table_name} MODIFY SETTING polling_min_timeout_ms=111
    """
    )
    assert 111 == int(
        node.query(
            f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'polling_min_timeout_ms'"
        )
    )

    node.query(
        f"""
        ALTER TABLE {table_name} MODIFY SETTING polling_min_timeout_ms=222, polling_max_timeout_ms=333
    """
    )
    assert 222 == int(
        node.query(
            f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'polling_min_timeout_ms'"
        )
    )
    assert 333 == int(
        node.query(
            f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'polling_max_timeout_ms'"
        )
    )

    assert "polling_max_timeout_ms = 333" in node.query(
        f"SHOW CREATE TABLE {table_name}"
    )

    node.restart_clickhouse()

    assert "polling_max_timeout_ms = 333" in node.query(
        f"SHOW CREATE TABLE {table_name}"
    )

    assert 333 == int(
        node.query(
            f"SELECT value FROM system.s3_queue_settings WHERE table = '{table_name}' and name = 'polling_max_timeout_ms'"
        )
    )
    node.query(f"DROP TABLE {table_name} SYNC")


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
        node.query("DROP DATABASE IF EXISTS r")

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
        assert expected_rows[0] == get_count()

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


@pytest.mark.parametrize("mode", ["unordered", "ordered"])
def test_filtering_files(started_cluster, mode):
    node1 = started_cluster.instances["instance"]
    node2 = started_cluster.instances["instance2"]

    table_name = f"test_replicated_{mode}_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 100

    node1.query("DROP DATABASE IF EXISTS r")
    node2.query("DROP DATABASE IF EXISTS r")

    node1.query(
        f"CREATE DATABASE r ENGINE=Replicated('/clickhouse/databases/{table_name}', 'shard1', 'node1')"
    )
    node2.query(
        f"CREATE DATABASE r ENGINE=Replicated('/clickhouse/databases/{table_name}', 'shard1', 'node2')"
    )

    create_table(
        started_cluster,
        node1,
        table_name,
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "polling_min_timeout_ms": 100,
            "polling_max_timeout_ms": 100,
            "polling_backoff_ms": 0,
        },
        database_name="r",
    )

    files = [(f"{files_path}/test_{i}.csv", i) for i in range(0, files_to_generate)]
    total_values = generate_random_files(
        started_cluster,
        files_path,
        files_to_generate,
        start_ind=0,
        row_num=1,
        files=files,
    )
    incorrect_values = [
        ["failed", 1, 1],
    ]
    incorrect_values_csv = (
        "\n".join((",".join(map(str, row)) for row in incorrect_values)) + "\n"
    ).encode()

    failed_file = f"{files_path}/testz_fff.csv"
    put_s3_file_content(started_cluster, failed_file, incorrect_values_csv)

    create_mv(node1, f"r.{table_name}", dst_table_name)

    def get_count():
        return int(node1.query(f"SELECT count() FROM default.{dst_table_name}"))

    expected_rows = files_to_generate
    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)
    assert expected_rows == get_count()

    create_mv(node2, f"r.{table_name}", dst_table_name)
    for _ in range(20):
        if node2.contains_in_log(f"StorageS3Queue (r.{table_name}): Processed rows: 0"):
            break
        time.sleep(1)
    assert node2.contains_in_log(f"StorageS3Queue (r.{table_name}): Processed rows: 0")

    found_1_global = False
    found_2_global = False
    if mode == "unordered":
        is_unordered = True
    else:
        is_unordered = False

    for file in files:
        found_1 = node2.contains_in_log(
            f"StorageS3Queue (r.{table_name}): Skipping file {file[0]}: Processed"
        )
        found_1_global = found_1_global or found_1

        if is_unordered:
            found_2 = node2.contains_in_log(
                f"Will skip file {file[0]}: it should be processed by"
            )
            found_2_global = found_2_global or found_2
        else:
            found_2 = False

        assert found_1 or found_2, "Failed with file " + file[0]

    assert found_1_global
    if is_unordered:
        assert found_2_global

    assert node2.contains_in_log(
        f"StorageS3Queue (r.{table_name}): Skipping file {failed_file}: Failed"
    ) or node1.contains_in_log(
        f"StorageS3Queue (r.{table_name}): Skipping file {failed_file}: Failed"
    )


def test_failed_commit(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_failed_commit_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 1

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
        },
    )
    total_values = generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=2
    )

    node.query(f"SYSTEM ENABLE FAILPOINT object_storage_queue_fail_commit")

    create_mv(node, table_name, dst_table_name)

    def check_failpoint():
        return node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Failed to process data: Code: 1002. DB::Exception: Failed to commit processed files. (UNKNOWN_EXCEPTION)"
        )

    for _ in range(100):
        if check_failpoint():
            break
        time.sleep(1)

    assert check_failpoint()

    node.query("SYSTEM FLUSH LOGS")
    assert 0 == int(
        node.query(
            f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
        )
    )

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    count_failed = int(
        node.count_in_log(
            f"StorageS3Queue (default.{table_name}): Failed to process data: Code: 1002. DB::Exception: Failed to commit processed files. (UNKNOWN_EXCEPTION)"
        )
    )
    count = get_count()
    expected_rows = 2 * count_failed
    expected_rows_upper = 2 * (
        count_failed + 2
    )  # Could get more in between getting 'count_failed' and getting 'count'

    assert expected_rows <= count and count <= expected_rows_upper

    node.query(f"SYSTEM DISABLE FAILPOINT object_storage_queue_fail_commit")

    processed = False
    for _ in range(20):
        node.query("SYSTEM FLUSH LOGS")
        processed = int(
            node.query(
                f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
            )
        )
        if processed == 1:
            break
        time.sleep(1)

    assert processed == 1
    assert 2 == int(
        node.query(
            f"SELECT rows_processed FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
        )
    )


def test_failure_in_the_middle(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_failure_in_the_middle_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}_{generate_random_string()}"
    files_path = f"{table_name}_data"
    files_to_generate = 1

    format = "column1 String, column2 String"
    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        format=format,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 10000,
        },
    )
    values = []
    num_rows = 1000000
    for _ in range(num_rows):
        values.append(
            ["".join("a" for i in range(1000)), "".join("a" for i in range(1000))]
        )
    values_csv = (
        "\n".join((",".join(map(str, row)) for row in values)) + "\n"
    ).encode()

    file_name = f"{table_name}_file.csv"
    put_s3_file_content(started_cluster, f"{files_path}/{file_name}", values_csv)

    node.query(
        f"SYSTEM ENABLE FAILPOINT object_storage_queue_fail_in_the_middle_of_file"
    )

    create_mv(node, table_name, dst_table_name, format=format)

    def check_failpoint():
        return node.contains_in_log(
            f"StorageS3Queue (default.{table_name}): Got an error while pulling chunk: Code: 1002. DB::Exception: Failed to read file. Processed rows:"
        )

    for _ in range(40):
        if check_failpoint():
            break
        time.sleep(1)

    assert check_failpoint()

    node.query("SYSTEM FLUSH LOGS")
    assert 0 == int(
        node.query(
            f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
        )
    )

    assert 1 <= int(
        node.query(
            f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Failed' and exception ilike '%Failed to read file. Processed rows%'"
        )
    )

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    assert 0 == get_count()

    node.query(
        f"SYSTEM DISABLE FAILPOINT object_storage_queue_fail_in_the_middle_of_file"
    )

    processed = False
    for _ in range(40):
        node.query("SYSTEM FLUSH LOGS")
        processed = int(
            node.query(
                f"SELECT count() FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
            )
        )
        if processed == 1:
            break
        time.sleep(1)

    assert processed == 1
    assert num_rows == int(
        node.query(
            f"SELECT rows_processed FROM system.s3queue_log WHERE table = '{table_name}' and status = 'Processed'"
        )
    )


def test_disable_streaming(started_cluster):
    node = started_cluster.instances["instance"]

    table_name = f"test_disable_streaming_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    files_to_generate = 10

    assert (
        "false"
        == node.query("SELECT getServerSetting('s3queue_disable_streaming')").strip()
    )

    node.replace_in_config(
        "/etc/clickhouse-server/config.d/disable_streaming.xml",
        "0",
        "1",
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert (
        "true"
        == node.query("SELECT getServerSetting('s3queue_disable_streaming')").strip()
    )

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "processing_threads_num": 1,
            "keeper_path": keeper_path,
        },
    )

    generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=1
    )

    create_mv(node, table_name, dst_table_name)

    def get_count():
        return int(node.query(f"SELECT count() FROM {dst_table_name}"))

    assert node.contains_in_log(
        f"StorageS3Queue (default.{table_name}): Streaming is disabled, rescheduling next check in 5000 ms"
    )
    assert 0 == get_count()

    assert (
        "true"
        == node.query("SELECT getServerSetting('s3queue_disable_streaming')").strip()
    )

    node.replace_in_config(
        "/etc/clickhouse-server/config.d/disable_streaming.xml",
        "1",
        "0",
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert (
        "false"
        == node.query("SELECT getServerSetting('s3queue_disable_streaming')").strip()
    )

    expected_rows = files_to_generate
    for _ in range(20):
        if expected_rows == get_count():
            break
        time.sleep(1)

    assert expected_rows == get_count()
