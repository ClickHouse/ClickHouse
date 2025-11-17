import logging
import os

import pytest

import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.mock_servers import start_mock_servers


def run_s3_mocks(started_cluster, args=[]):
    script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
    start_mock_servers(
        started_cluster,
        script_dir,
        [("mock_sts.py", "sts.amazonaws.com", "80", args)],
    )


@pytest.fixture(scope="module")
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
                "configs/remote_servers_test_shared_localhost.xml",
                "configs/named_collections.xml",
                "configs/s3_credentials_provider_cache_size.xml",
            ],
            user_configs=["configs/users.xml"],
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


def test_sts_smoke(started_cluster):
    instance = started_cluster.instances["s3_with_environment_credentials"]

    instance.query(
        f"""
        INSERT INTO FUNCTION s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_smoke.csv', 'minio', '{minio_secret_key}')
        SELECT number, number * 10, number * 100 FROM numbers(10) SETTINGS s3_truncate_on_insert = 1"""
    )

    with pytest.raises(helpers.client.QueryRuntimeException) as ei:
        instance.query(
            f"""
            SELECT sum(a), sum(b), sum(c) FROM s3(
                'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_smoke.csv',
                'CSV', 'a Int64, b Int64, c Int64',
                extra_credentials(role_arn = 'arn::role', role_session_name = 'mysession'))
                SETTINGS s3_max_single_read_retries = 1, s3_retry_attempts = 1, s3_request_timeout_ms = 1000
        """
        )

    assert ei.value.returncode == 243
    assert "HTTP response code: 403" in ei.value.stderr

    assert "45\t450\t4500\n" == instance.query(
        f"""
        SELECT sum(a), sum(b), sum(c) FROM s3(
            'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_smoke.csv',
            'CSV', 'a Int64, b Int64, c Int64',
            extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole'))
    """
    )


def test_sts_smoke_s3cluster(started_cluster):
    instance = started_cluster.instances["s3_with_environment_credentials"]

    instance.query(
        f"""
        INSERT INTO FUNCTION s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_smoke_s3cluster.csv', 'minio', '{minio_secret_key}')
        SELECT number, number * 10, number * 100 FROM numbers(10) SETTINGS s3_truncate_on_insert = 1"""
    )

    with pytest.raises(helpers.client.QueryRuntimeException) as ei:
        instance.query(
            f"""
            SELECT sum(a), sum(b), sum(c) FROM s3Cluster(
                test_shard_localhost,
                'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_smoke_s3cluster.csv',
                'CSV', extra_credentials(role_arn = 'arn::role', role_session_name = 'mysession'))
                SETTINGS s3_max_single_read_retries = 1, s3_retry_attempts = 1, s3_request_timeout_ms = 1000
        """
        )

    assert ei.value.returncode == 243
    assert "DB::Exception: Failed to get object info" in ei.value.stderr

    assert "45\t450\t4500\n" == instance.query(
        f"""
        SELECT sum(c1), sum(c2), sum(c3) FROM s3Cluster(
            test_shard_localhost,
            'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_smoke_s3cluster.csv',
            'CSV', extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole'))
    """
    )


def test_sts_credentials_cache(started_cluster):
    instance = started_cluster.instances["s3_with_environment_credentials"]

    instance.query(
        f"""
        INSERT INTO FUNCTION s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_smoke.csv', 'minio', '{minio_secret_key}')
        SELECT number, number * 10, number * 100 FROM numbers(10) SETTINGS s3_truncate_on_insert = 1"""
    )

    for i in range(20):
        assert "45\t450\t4500\n" == instance.query(
            f"""
            SELECT sum(a), sum(b), sum(c) FROM s3(
                'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_smoke.csv',
                'CSV', 'a Int64, b Int64, c Int64',
                extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole'))
        """
        )

    added = int(
        instance.query(
            "SELECT value FROM system.events WHERE event = 'S3CachedCredentialsProvidersAdded'"
        )
    )
    reused = int(
        instance.query(
            "SELECT value FROM system.events WHERE event = 'S3CachedCredentialsProvidersReused'"
        )
    )

    assert added > 0 and reused > 0
    assert reused > added

    assert (
        int(
            instance.query(
                "SELECT value FROM system.metrics WHERE name = 'S3CachedCredentialsProviders'"
            )
        )
        > 0
    )

    for i in range(20):
        with pytest.raises(helpers.client.QueryRuntimeException) as ei:
            instance.query(
                f"""
                SELECT sum(a), sum(b), sum(c) FROM s3Cluster(
                    test_shard_localhost,
                    'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_smoke_s3cluster.csv',
                    'CSV', extra_credentials(role_arn = 'arn::role', role_session_name = 'mysession{i}'))
                    SETTINGS s3_max_single_read_retries = 1, s3_retry_attempts = 1, s3_request_timeout_ms = 1000
            """
            )

    assert (
        int(
            instance.query(
                "SELECT value FROM system.metrics WHERE name = 'S3CachedCredentialsProviders'"
            )
        )
        == 10
    )
