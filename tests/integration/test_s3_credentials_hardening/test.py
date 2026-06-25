import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key

# The server is given S3 credentials by server-managed mechanisms that user SQL must not be able to reuse
# when `s3_allow_server_credentials_in_user_queries` is disabled (the default). Here we exercise the AWS
# shared-credentials file (the ProfileConfigFileAWSCredentialsProvider): it points at the working minio
# credentials, and AWS_EC2_METADATA_DISABLED plus the absence of AWS_ACCESS_KEY_ID make it the only
# credential source, so the test isolates that provider specifically.
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_minio=True,
    main_configs=["configs/named_collections.xml"],
    user_configs=["configs/users.xml"],
    env_variables={
        "AWS_SHARED_CREDENTIALS_FILE": "/tmp/aws_credentials",
        "AWS_EC2_METADATA_DISABLED": "true",
    },
)

# A separate instance whose server <s3> config carries a global role_arn (and no environment
# credentials), to verify that user-facing S3 entry points do not inherit it. Separate because a global
# role_arn would interfere with the profile-file tests above once the override is enabled.
node_with_server_role = cluster.add_instance(
    "node_with_server_role",
    with_minio=True,
    main_configs=["configs/named_collections.xml", "configs/s3_server_role.xml"],
    user_configs=["configs/users.xml"],
    env_variables={
        "AWS_EC2_METADATA_DISABLED": "true",
    },
)

# A separate instance whose server <s3> config carries static keys (and no environment credentials), to
# verify that a backup named collection overrides -- rather than inherits -- the server's static keys.
node_with_server_keys = cluster.add_instance(
    "node_with_server_keys",
    with_minio=True,
    main_configs=["configs/named_collections.xml", "configs/s3_server_keys.xml"],
    user_configs=["configs/users.xml"],
    env_variables={
        "AWS_EC2_METADATA_DISABLED": "true",
    },
)

ALLOW = "SETTINGS s3_allow_server_credentials_in_user_queries = 1"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        # Stand in for the server's own AWS profile credentials.
        node.exec_in_container(
            [
                "bash",
                "-c",
                "printf '[default]\\naws_access_key_id = %s\\naws_secret_access_key = %s\\n' > /tmp/aws_credentials"
                % (minio_access_key, minio_secret_key),
            ]
        )
        yield
    finally:
        cluster.shutdown()


def test_profile_file_not_used_for_user_queries():
    # No explicit keys, no env/IMDS credentials: the only possible source is the AWS shared-credentials
    # file (ProfileConfigFileAWSCredentialsProvider). It must not be used by default.
    url = "http://minio1:9001/root/profilecreds/data.tsv"
    insert = (
        f"INSERT INTO FUNCTION s3('{url}', format = 'TSV', structure = 'x UInt8') "
        f"SELECT 1 SETTINGS s3_truncate_on_insert = 1"
    )

    error = node.query_and_get_error(insert)
    assert "ACCESS_DENIED" in error, error

    # With the setting enabled, the profile file is used and the request succeeds (proving the profile
    # file really was the only available credential source).
    node.query(insert + ", s3_allow_server_credentials_in_user_queries = 1")
    assert (
        node.query(
            f"SELECT * FROM s3('{url}', format = 'TSV', structure = 'x UInt8') {ALLOW}"
        ).strip()
        == "1"
    )


def test_url_only_named_collection_stays_anonymous():
    # A URL-only named collection means anonymous access. Even when a trusted session re-enables
    # server-managed credentials, the collection must not silently pick up the server's AWS
    # shared-credentials file; `use_environment_credentials = 1` is the explicit opt-in.
    url = "http://minio1:9001/root/profilecreds_nc/data.tsv"
    node.query(
        f"INSERT INTO FUNCTION s3('{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8') "
        "SELECT 7 SETTINGS s3_truncate_on_insert = 1"
    )

    select_url_only = (
        "SELECT * FROM s3(nc_url_only, format = 'TSV', structure = 'x UInt8')"
    )

    # Anonymous by default, and still anonymous with the override enabled: minio rejects the unsigned
    # read of the private bucket instead of accepting the server profile credentials.
    error = node.query_and_get_error(select_url_only)
    assert "403" in error or "Access Denied" in error or "AccessDenied" in error, error
    error = node.query_and_get_error(f"{select_url_only} {ALLOW}")
    assert "403" in error or "Access Denied" in error or "AccessDenied" in error, error

    # The explicit opt-in keeps working: with the override enabled and `use_environment_credentials = 1`
    # the profile file is consulted and the read succeeds.
    select_with_env = (
        "SELECT * FROM s3(nc_url_only, use_environment_credentials = 1, "
        "format = 'TSV', structure = 'x UInt8')"
    )
    error = node.query_and_get_error(select_with_env)
    assert "ACCESS_DENIED" in error, error
    assert node.query(f"{select_with_env} {ALLOW}").strip() == "7"


def test_backup_named_collection_does_not_inherit_server_role():
    node_with_server_role.query("DROP TABLE IF EXISTS t_backup SYNC")
    node_with_server_role.query(
        "CREATE TABLE t_backup (x UInt8) ENGINE = MergeTree ORDER BY tuple()"
    )
    node_with_server_role.query("INSERT INTO t_backup SELECT 1")

    # Control: the positional URL form inherits the global <s3> role_arn, a server-managed credential
    # source, so the restriction rejects it. This also proves the role config is loaded and visible to
    # backups.
    error = node_with_server_role.query_and_get_error(
        "BACKUP TABLE t_backup TO S3('http://minio1:9001/root/backup_positional/b1')"
    )
    assert "server-managed credentials" in error, error

    # A URL-only backup named collection is a full auth override: the global role_arn must not leak into
    # it, so the backup goes out unsigned and minio rejects the anonymous request instead.
    error = node_with_server_role.query_and_get_error(
        "BACKUP TABLE t_backup TO S3(nc_backup_url_only, 'b1')"
    )
    assert "server-managed credentials" not in error, error
    assert "403" in error or "Access Denied" in error or "AccessDenied" in error, error

    node_with_server_role.query("DROP TABLE t_backup SYNC")


def test_backup_named_collection_gcp_oauth_is_rejected():
    # A backup named collection that itself asks for `http_client = gcp_oauth` without a complete explicit
    # Google ADC triple must reach the central credential rejection -- it would otherwise mint a token from
    # the server's GCP metadata service. It must not be silently downgraded to anonymous (the strip in
    # makeS3Client only drops a `gcp_oauth` inherited from the server <s3> config, not one the collection
    # supplied).
    node.query("DROP TABLE IF EXISTS t_backup_gcp SYNC")
    node.query("CREATE TABLE t_backup_gcp (x UInt8) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO t_backup_gcp SELECT 1")

    error = node.query_and_get_error(
        "BACKUP TABLE t_backup_gcp TO S3(nc_backup_gcp_oauth, 'b1')"
    )
    # The specific credential-restriction message proves it is the restriction rejecting it, not anonymous
    # access failing or `gcp_oauth` being unsupported.
    assert "gcp_oauth" in error and "not allowed to use" in error, error

    node.query("DROP TABLE t_backup_gcp SYNC")


def test_backup_named_collection_does_not_inherit_server_keys():
    node_with_server_keys.query("DROP TABLE IF EXISTS t_backup_keys SYNC")
    node_with_server_keys.query(
        "CREATE TABLE t_backup_keys (x UInt8) ENGINE = MergeTree ORDER BY tuple()"
    )
    node_with_server_keys.query("INSERT INTO t_backup_keys SELECT 1")

    # Control: the positional URL form inherits the server <s3> static keys, so the backup succeeds. This
    # proves the server keys are configured and usable for this endpoint (static keys are explicit
    # credentials, not a server-managed mechanism, so the restriction does not block them).
    node_with_server_keys.query(
        "BACKUP TABLE t_backup_keys TO S3('http://minio1:9001/root/backup_keys_positional/b1')"
    )

    # A URL-only backup named collection is a full auth override: the server static keys must not leak into
    # it, so the backup goes out unsigned and minio rejects the anonymous request instead.
    error = node_with_server_keys.query_and_get_error(
        "BACKUP TABLE t_backup_keys TO S3(nc_backup_url_only, 'b1')"
    )
    assert "403" in error or "Access Denied" in error or "AccessDenied" in error, error

    # A backup named collection with only a role_arn has no base key pair of its own, so the server static
    # keys must not be used as the STS base to assume the role. The role cannot be assumed and the backup
    # fails (unsigned -> rejected) instead of authenticating with the server's keys.
    error = node_with_server_keys.query_and_get_error(
        "BACKUP TABLE t_backup_keys TO S3(nc_backup_role_only, 'b1')"
    )
    assert (
        "403" in error
        or "Access Denied" in error
        or "AccessDenied" in error
        or "server-managed credentials" in error
    ), error

    node_with_server_keys.query("DROP TABLE t_backup_keys SYNC")


def test_server_data_disk_unaffected():
    # The server's own S3-backed operations are not user queries and keep working regardless.
    node.query("DROP TABLE IF EXISTS t_local SYNC")
    node.query("CREATE TABLE t_local (x UInt8) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO t_local SELECT 4")
    assert node.query("SELECT sum(x) FROM t_local").strip() == "4"
    node.query("DROP TABLE t_local SYNC")
