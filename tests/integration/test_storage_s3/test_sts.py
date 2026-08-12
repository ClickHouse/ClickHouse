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
            user_configs=[
                "configs/users.xml",
                "configs/sync_insert.xml",
                # Deliberately no s3_allow_server_credentials_in_user_queries opt-in: role_arn-based STS
                # assume-role must work under the default restriction (the AssumeRole call is signed with the
                # server's ambient credentials; only the assumed role's credentials sign the S3 requests).
            ],
        )

        # Global <s3> config disables use_environment_credentials (so a bare, credential-less
        # s3(url) stays anonymous by default) and no s3_allow_server_credentials_in_user_queries
        # opt-in. A role_arn-based assume-role must still work: its STS base resolution is forced on
        # internally for the role_arn case regardless of use_environment_credentials.
        cluster.add_instance(
            "s3_with_environment_credentials_disabled",
            with_minio=True,
            env_variables={
                "AWS_ACCESS_KEY_ID": "aws",
                "AWS_SECRET_ACCESS_KEY": "aws123",
            },
            main_configs=[
                "configs/use_environment_credentials_disabled.xml",
                "configs/remote_servers_test_shared_localhost.xml",
                "configs/named_collections.xml",
                "configs/s3_credentials_provider_cache_size.xml",
            ],
            user_configs=[
                "configs/users.xml",
                "configs/sync_insert.xml",
            ],
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


def test_sts_smoke_env_credentials_disabled_no_opt_in(started_cluster):
    # role_arn-based STS assume-role with no explicit keys at all, against a node whose global <s3>
    # config disables use_environment_credentials and whose default profile carries no
    # s3_allow_server_credentials_in_user_queries opt-in. Both SELECT and INSERT INTO FUNCTION forms
    # must work: the AssumeRole call is signed with the server's ambient credentials and only the
    # assumed role's credentials sign the S3 requests.
    instance = started_cluster.instances["s3_with_environment_credentials_disabled"]

    instance.query(
        f"""
        INSERT INTO FUNCTION s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_prod_shape.csv', 'minio', '{minio_secret_key}')
        SELECT number, number * 10, number * 100 FROM numbers(10) SETTINGS s3_truncate_on_insert = 1"""
    )

    # SELECT with role_arn and no explicit keys at all.
    assert "45\t450\t4500\n" == instance.query(
        f"""
        SELECT sum(a), sum(b), sum(c) FROM s3(
            'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_prod_shape.csv',
            'CSV', 'a Int64, b Int64, c Int64',
            extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole'))
    """
    )

    # INSERT INTO FUNCTION with role_arn and no explicit keys: the multipart-upload write path must
    # also use the assumed role (not go out anonymous, which S3 rejects for multipart uploads).
    instance.query(
        f"""
        INSERT INTO FUNCTION s3(
            'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_prod_shape_write.csv',
            'CSV', 'a Int64, b Int64, c Int64',
            extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole'))
        SELECT number, number * 10, number * 100 FROM numbers(10) SETTINGS s3_truncate_on_insert = 1
    """
    )
    assert "45\t450\t4500\n" == instance.query(
        f"""
        SELECT sum(a), sum(b), sum(c) FROM s3(
            'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_prod_shape_write.csv',
            'minio', '{minio_secret_key}', 'CSV', 'a Int64, b Int64, c Int64')
    """
    )

    # Control: a wrong session name still fails 403 (proves the role is actually being assumed and
    # checked, not silently bypassed).
    with pytest.raises(helpers.client.QueryRuntimeException) as ei:
        instance.query(
            f"""
            SELECT sum(a), sum(b), sum(c) FROM s3(
                'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_prod_shape.csv',
                'CSV', 'a Int64, b Int64, c Int64',
                extra_credentials(role_arn = 'arn::role', role_session_name = 'mysession'))
                SETTINGS s3_max_single_read_retries = 1, s3_retry_attempts = 1, s3_request_timeout_ms = 1000
        """
        )
    assert ei.value.returncode == 243
    assert "HTTP response code: 403" in ei.value.stderr


def test_sts_external_id(started_cluster):
    instance = started_cluster.instances["s3_with_environment_credentials"]

    instance.query(
        f"""
        INSERT INTO FUNCTION s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_external_id.csv', 'minio', '{minio_secret_key}')
        SELECT number, number * 10, number * 100 FROM numbers(10) SETTINGS s3_truncate_on_insert = 1"""
    )

    # Negative: the role session is accepted by the mock STS, but the supplied
    # external id does not match, so AssumeRole returns credentials minio rejects.
    # This only fails if `external_id` actually reaches the AssumeRole request.
    with pytest.raises(helpers.client.QueryRuntimeException) as ei:
        instance.query(
            f"""
            SELECT sum(a), sum(b), sum(c) FROM s3(
                'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_external_id.csv',
                'CSV', 'a Int64, b Int64, c Int64',
                extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole', external_id = 'wrong_external_id'))
                SETTINGS s3_max_single_read_retries = 1, s3_retry_attempts = 1, s3_request_timeout_ms = 1000
        """
        )

    assert ei.value.returncode == 243
    assert "HTTP response code: 403" in ei.value.stderr

    # Positive: matching role session name and external id yield working credentials.
    assert "45\t450\t4500\n" == instance.query(
        f"""
        SELECT sum(a), sum(b), sum(c) FROM s3(
            'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_external_id.csv',
            'CSV', 'a Int64, b Int64, c Int64',
            extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole', external_id = 'miniexternalid'))
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
        with pytest.raises(helpers.client.QueryRuntimeException):
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


def test_sts_backup_restore(started_cluster):
    # BACKUP/RESTORE TO S3 assembles its S3 credentials separately from the s3()/s3Cluster() path
    # (registerBackupEngineS3 / BackupIO_S3), so it needs its own positive coverage: a role_arn-based
    # STS assume-role destination, no explicit keys, and no s3_allow_server_credentials_in_user_queries
    # opt-in. The AssumeRole is signed with the server's ambient credentials and only the assumed
    # role's credentials sign the backup writes/reads.
    instance = started_cluster.instances["s3_with_environment_credentials_disabled"]
    backup_url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_sts_backup"

    instance.query("DROP TABLE IF EXISTS t_sts_backup SYNC")
    instance.query("CREATE TABLE t_sts_backup (x UInt64) ENGINE = MergeTree ORDER BY x")
    instance.query("INSERT INTO t_sts_backup SELECT number FROM numbers(100)")

    # Wrong role session name: the assume yields credentials MinIO rejects, so the backup fails at S3
    # (not the restriction). Proves the role is actually assumed and used, not bypassed.
    with pytest.raises(helpers.client.QueryRuntimeException) as ei:
        instance.query(
            f"""
            BACKUP TABLE t_sts_backup TO S3(
                '{backup_url}_wrong',
                extra_credentials(role_arn = 'arn::role', role_session_name = 'mysession'))
            SETTINGS s3_max_single_read_retries = 1, s3_retry_attempts = 1, s3_request_timeout_ms = 1000
        """
        )
    assert "server-managed credentials" not in ei.value.stderr, ei.value.stderr
    assert "403" in ei.value.stderr or "Access Denied" in ei.value.stderr, ei.value.stderr

    # Correct role session name: the assume yields working credentials, so the backup succeeds.
    assert "BACKUP_CREATED" in instance.query(
        f"""
        BACKUP TABLE t_sts_backup TO S3(
            '{backup_url}',
            extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole'))
    """
    )

    instance.query("DROP TABLE t_sts_backup SYNC")

    # RESTORE goes through the same role_arn STS path on the read side and must recover the data.
    assert "RESTORED" in instance.query(
        f"""
        RESTORE TABLE t_sts_backup FROM S3(
            '{backup_url}',
            extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole'))
    """
    )
    assert "100" == instance.query("SELECT count() FROM t_sts_backup").strip()
    assert "4950" == instance.query("SELECT sum(x) FROM t_sts_backup").strip()

    instance.query("DROP TABLE t_sts_backup SYNC")


def test_role_arn_override_drops_collection_external_id(started_cluster):
    # A query that overrides `role_arn` on a named collection must not silently inherit the
    # collection's `external_id` -- it is the secret half of the STS triple, tied to the
    # collection's own role. The `s3_role_extid_leak` collection carries a deliberately wrong
    # external_id (and role_session_name = 'miniorole') but no role_arn. The query supplies its own
    # role_arn and does not override external_id: the read succeeds only if the collection's wrong
    # external_id was dropped (the mock STS accepts a request with no ExternalId), and fails with a
    # 403 if it leaked into the AssumeRole call.
    instance = started_cluster.instances["s3_with_environment_credentials"]
    url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/test_role_extid_leak.csv"

    instance.query(
        f"""
        INSERT INTO FUNCTION s3('{url}', 'minio', '{minio_secret_key}')
        SELECT number, number * 10, number * 100 FROM numbers(10) SETTINGS s3_truncate_on_insert = 1"""
    )

    assert "45\t450\t4500\n" == instance.query(
        """
        SELECT sum(a), sum(b), sum(c) FROM s3(
            s3_role_extid_leak,
            role_arn = 'arn::role',
            format = 'CSV', structure = 'a Int64, b Int64, c Int64')
        SETTINGS s3_max_single_read_retries = 1, s3_retry_attempts = 1, s3_request_timeout_ms = 10000
    """
    )
