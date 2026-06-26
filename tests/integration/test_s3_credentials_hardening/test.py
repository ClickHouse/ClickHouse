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

# A separate instance whose server <s3> config carries a per-endpoint session_token, to verify that an
# explicit-key user query does not inherit (and send) the server's temporary token.
node_with_server_session_token = cluster.add_instance(
    "node_with_server_session_token",
    with_minio=True,
    main_configs=["configs/named_collections.xml", "configs/s3_session_token.xml"],
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


def test_named_collection_role_arn_override_does_not_use_collection_keys():
    # A config collection carries operator-provisioned static keys. With named-collection overrides enabled
    # (the default), a user must not be able to add `role_arn = ...` to assume an arbitrary role using those
    # keys as the STS base. Under the restriction the injected role is dropped and the collection's keys are
    # used directly (equivalent to s3(collection)).
    url = "http://minio1:9001/root/nc_role_override/data.tsv"
    node.query(
        f"INSERT INTO FUNCTION s3('{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8') "
        "SELECT 5 SETTINGS s3_truncate_on_insert = 1"
    )

    # The injected role is dropped, so the read succeeds using the collection's own keys.
    assert (
        node.query(
            "SELECT * FROM s3(nc_with_keys, role_arn = 'arn:aws:iam::123456789012:role/evil', "
            "format = 'TSV', structure = 'x UInt8')"
        ).strip()
        == "5"
    )

    # With the override enabled the injected role is honored, so the role assume is attempted (and fails,
    # there is no STS endpoint) instead of silently reading with the collection keys. This proves the read
    # above succeeded because the role was dropped, not because the role was harmlessly ignored.
    error = node.query_and_get_error(
        "SELECT * FROM s3(nc_with_keys, role_arn = 'arn:aws:iam::123456789012:role/evil', "
        f"format = 'TSV', structure = 'x UInt8') {ALLOW}"
    )
    assert error, error


def test_database_s3_named_collection_use_environment_credentials():
    # ENGINE = S3(named_collection) must apply the same restriction as the s3 table function it flattens into.
    # A collection opting in with use_environment_credentials = 1 is refused by default and works with the
    # session opt-in -- proving the key is whitelisted (no unexpected-key error) and carried through to the
    # built s3() table function (not flattened away).
    url = "http://minio1:9001/root/db_s3_env_test/data.tsv"
    node.query(
        f"INSERT INTO FUNCTION s3('{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8') "
        "SELECT 9 SETTINGS s3_truncate_on_insert = 1"
    )

    node.query("DROP DATABASE IF EXISTS db_s3_env SYNC")
    node.query("CREATE DATABASE db_s3_env ENGINE = S3(nc_db_use_env)")

    # Default restriction: building the s3() table resolves the server's ambient credentials, so it is refused.
    error = node.query_and_get_error("SELECT * FROM db_s3_env.`data.tsv`")
    assert "ACCESS_DENIED" in error, error

    # With the session opt-in the ambient credentials are used and the read succeeds.
    assert node.query(f"SELECT * FROM db_s3_env.`data.tsv` {ALLOW}").strip() == "9"

    node.query("DROP DATABASE db_s3_env SYNC")


def test_explicit_keys_drop_inherited_session_token():
    # The endpoint carries a (bogus) server session_token in <s3> config. A user query that supplies its own
    # explicit key pair must not inherit and send that token -- otherwise minio rejects the request. The read
    # succeeding proves the inherited token was dropped.
    url = "http://minio1:9001/root/sessiontoken/data.tsv"
    # Write with explicit keys from a node without the session_token config.
    node.query(
        f"INSERT INTO FUNCTION s3('{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8') "
        "SELECT 3 SETTINGS s3_truncate_on_insert = 1"
    )

    assert (
        node_with_server_session_token.query(
            f"SELECT * FROM s3('{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8')"
        ).strip()
        == "3"
    )


def test_backup_role_arn_override_does_not_use_collection_keys():
    # A query-overridden `role_arn` on a backup collection that carries operator-provisioned keys must not be
    # assumed using those keys as the STS base. Under the restriction the injected role is dropped and the
    # collection's keys are used directly, so the backup succeeds.
    node.query("DROP TABLE IF EXISTS t_backup_override SYNC")
    node.query("CREATE TABLE t_backup_override (x UInt8) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO t_backup_override SELECT 1")

    node.query(
        "BACKUP TABLE t_backup_override TO "
        "S3(nc_backup_with_keys, role_arn = 'arn:aws:iam::123456789012:role/evil', 'b_override')"
    )

    # With the override enabled the injected role is honored, so the role assume is attempted (and fails) instead
    # of silently using the collection keys -- proving the success above was the role being dropped.
    error = node.query_and_get_error(
        "BACKUP TABLE t_backup_override TO "
        f"S3(nc_backup_with_keys, role_arn = 'arn:aws:iam::123456789012:role/evil', 'b_override_allowed') {ALLOW}"
    )
    assert error

    node.query("DROP TABLE t_backup_override SYNC")


def test_backup_role_only_collection_is_rejected():
    # A backup collection that defines only a `role_arn` (no base keys) would assume the role using the server's
    # ambient credentials, so under the restriction it must reach the central rejection, not go anonymous.
    node.query("DROP TABLE IF EXISTS t_backup_role_only SYNC")
    node.query("CREATE TABLE t_backup_role_only (x UInt8) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO t_backup_role_only SELECT 1")

    error = node.query_and_get_error(
        "BACKUP TABLE t_backup_role_only TO S3(nc_backup_role_only, 'b_role_only')"
    )
    assert "server-managed credentials" in error, error

    node.query("DROP TABLE t_backup_role_only SYNC")


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
