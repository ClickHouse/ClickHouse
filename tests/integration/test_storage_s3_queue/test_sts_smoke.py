import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from test_storage_s3.test_sts import run_s3_mocks
from helpers.s3_queue_common import generate_random_files, create_table, create_mv


@pytest.fixture(scope="function")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "s3_with_environment_credentials",
            with_minio=True,
            env_variables={
                "AWS_ACCESS_KEY_ID": "aws",
                "AWS_SECRET_ACCESS_KEY": "aws123",
            },
            main_configs=[
                "configs/use_environment_credentials.xml",
                "configs/zookeeper.xml",
                "configs/s3_credentials_cache.xml",
            ],
            user_configs=["configs/users.xml", "configs/allow_server_credentials.xml"],
            with_zookeeper=True,
            stay_alive=True,
        )
        sts = cluster.add_instance(
            name="sts.amazonaws.com",
            hostname="sts.amazonaws.com",
            image="clickhouse/python-bottle",
            tag="latest",
            stay_alive=True,
        )
        sts.stop_clickhouse(kill=True)

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        logging.info("S3 bucket created")
        run_s3_mocks(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def test_s3_queue_extra_credentials(started_cluster):
    node = started_cluster.instances["s3_with_environment_credentials"]
    table_name = "test_extra_credentials"
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
            "s3queue_processing_threads_num": 5,
        },
        auth="",
        extra_credentials="extra_credentials(role_arn = 'arn::role', role_session_name = 'mysession')",
    )

    assert "Could not list objects in bucket" in node.query_and_get_error(
        f"SELECT * FROM {table_name}"
    )

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_processing_threads_num": 5,
        },
        auth="",
        extra_credentials="extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole')",
    )

    assert 0 == int(node.query(f"SELECT count() FROM {table_name}"))

    generate_random_files(
        started_cluster, files_path, files_to_generate, start_ind=0, row_num=10
    )

    create_mv(node, table_name, dst_table_name)

    def get_count(node, table_name):
        return int(node.query(f"SELECT count() FROM {table_name}"))

    for _ in range(150):
        if get_count(node, dst_table_name) == 10:
            break
        time.sleep(1)

    assert get_count(node, dst_table_name) == 10

    assert (
        "extra_credentials(\\'role_arn\\' = \\'arn::role\\', \\'role_session_name\\' = \\'miniorole\\')"
        in node.query(f"SHOW CREATE TABLE {table_name}")
    )

    node.restart_clickhouse()

    assert (
        "extra_credentials(\\'role_arn\\' = \\'arn::role\\', \\'role_session_name\\' = \\'miniorole\\')"
        in node.query(f"SHOW CREATE TABLE {table_name}")
    )


def test_s3_queue_extra_credentials_backup(started_cluster):
    node = started_cluster.instances["s3_with_environment_credentials"]
    table_name = "test_extra_credentials_backup"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    generate_random_files(
        started_cluster, files_path, count=1, start_ind=0, row_num=10
    )

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
        },
        auth="",
        extra_credentials="extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole')",
        format='',
    )

    node.query(f"BACKUP TABLE {table_name} TO File('test_backup')")
    assert node.query("SELECT status FROM system.backups").strip() == 'BACKUP_CREATED'
    node.query(f"DROP TABLE {table_name} SYNC")
    # Kill sts server to simulate wrong credentials
    started_cluster.instances['sts.amazonaws.com'].stop()
    started_cluster.instances['sts.amazonaws.com'].start()
    run_s3_mocks(started_cluster, ['changedrole'])

    err = create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
        },
        auth="",
        extra_credentials="extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole')",
        expect_error=True,
        format='',
    )
    assert "Could not list objects in bucket" in err

    node.query(f"RESTORE TABLE {table_name} FROM File('test_backup')")
    assert "Could not list objects in bucket" in node.query_and_get_error(
        f"SELECT * FROM {table_name}"
    )

    # The error above alone cannot prove the restored table kept the
    # extra_credentials clause: the base environment credentials (aws/aws123)
    # are equally invalid for MinIO, so a restored table that silently dropped
    # the clause would fail with the same error. Pin the round trip explicitly.
    assert (
        "extra_credentials(\\'role_arn\\' = \\'arn::role\\', \\'role_session_name\\' = \\'miniorole\\')"
        in node.query(f"SHOW CREATE TABLE {table_name}")
    )

    # And prove the restored clause is effective: once the STS mock grants
    # 'miniorole' again, the restored table becomes readable — impossible with
    # the base environment credentials. The server must be restarted for that:
    # the assume-role provider inside the table's S3 client caches the
    # wrong-key credentials it fetched above until their (1 hour) expiration,
    # regardless of s3_credentials_cache.xml disabling the provider cache.
    started_cluster.instances["sts.amazonaws.com"].stop()
    started_cluster.instances["sts.amazonaws.com"].start()
    run_s3_mocks(started_cluster)
    node.restart_clickhouse()

    assert 10 == int(node.query(f"SELECT count() FROM {table_name}"))
