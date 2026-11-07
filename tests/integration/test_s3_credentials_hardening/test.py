import os

import pytest
import requests

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.mock_servers import start_mock_servers
from helpers.test_tools import assert_eq_with_retry

BEARER_TOKEN_PORT = 8085

# The server is given S3 credentials by server-managed mechanisms that user SQL must not be able to reuse
# when `s3_allow_server_credentials_in_user_queries` is disabled (the default). Here we exercise the AWS
# shared-credentials file (the ProfileConfigFileAWSCredentialsProvider): it points at the working minio
# credentials, and AWS_EC2_METADATA_DISABLED plus the absence of AWS_ACCESS_KEY_ID make it the only
# credential source, so the test isolates that provider specifically.
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_minio=True,
    main_configs=["configs/named_collections.xml", "configs/bearer_token_hosts.xml"],
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

# A separate instance whose server <s3> config carries a per-endpoint gcp_oauth plus a complete Google ADC
# triple, to verify that an explicit-key user query/backup does not inherit (and use) the server gcp_oauth.
node_with_server_gcp_oauth = cluster.add_instance(
    "node_with_server_gcp_oauth",
    with_minio=True,
    main_configs=["configs/named_collections.xml", "configs/s3_gcp_oauth.xml"],
    user_configs=["configs/users.xml"],
    env_variables={
        "AWS_EC2_METADATA_DISABLED": "true",
    },
)

# A separate instance whose server <s3> config carries an SSE-C customer key for an endpoint, to verify that a
# named collection with its own keys does not inherit (and send) the server's encryption key material.
node_with_server_sse = cluster.add_instance(
    "node_with_server_sse",
    with_minio=True,
    main_configs=["configs/named_collections.xml", "configs/s3_sse_c.xml"],
    user_configs=["configs/users.xml"],
    env_variables={
        "AWS_EC2_METADATA_DISABLED": "true",
    },
)

# A separate instance that allows `include` substitutions in dynamic disk definitions, to verify the
# post-include re-check inspects `locations.*` children (not only the disk root).
node_with_includes = cluster.add_instance(
    "node_with_includes",
    with_minio=True,
    main_configs=[
        "configs/named_collections.xml",
        "configs/dynamic_disk_include.xml",
        "configs/dynamic_disk_include_source.xml",
    ],
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

    # The positional URL form must not inherit the global <s3> role_arn: a server-configured role is not
    # applied to user requests under the restriction, so the backup goes out unsigned and minio rejects the
    # anonymous request (it is not an ACCESS_DENIED restriction rejection either).
    error = node_with_server_role.query_and_get_error(
        "BACKUP TABLE t_backup TO S3('http://minio1:9001/root/backup_positional/b1')"
    )
    assert "server-managed credentials" not in error, error
    assert "403" in error or "Access Denied" in error or "AccessDenied" in error, error

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
    # A config collection carries operator-provisioned static keys. A user may add `role_arn = ...` (STS
    # assume-role stays allowed under the restriction), but the role must not be assumed with those keys as
    # the STS base: the collection keys are dropped and the assume-role call is signed with the server's
    # ambient identity instead. Here the assume fails (there is no STS endpoint) and the request goes out
    # unsigned, so the query errors - it must NOT silently succeed by reading with the collection's keys.
    url = "http://minio1:9001/root/nc_role_override/data.tsv"
    node.query(
        f"INSERT INTO FUNCTION s3('{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8') "
        "SELECT 5 SETTINGS s3_truncate_on_insert = 1"
    )

    error = node.query_and_get_error(
        "SELECT * FROM s3(nc_with_keys, role_arn = 'arn:aws:iam::123456789012:role/evil', "
        "format = 'TSV', structure = 'x UInt8')"
    )
    # Not the restriction rejection (the role is allowed), and not a successful read with the collection keys.
    assert "server-managed credentials" not in error, error
    assert error, error

    # Control: without the injected role the collection keys are used directly and the read succeeds.
    assert (
        node.query("SELECT * FROM s3(nc_with_keys, format = 'TSV', structure = 'x UInt8')").strip()
        == "5"
    )

    # With the opt-in the injected role is honored with the collection keys as the STS base; the assume is
    # still attempted (and fails, there is no STS endpoint) instead of reading with the collection keys.
    error = node.query_and_get_error(
        "SELECT * FROM s3(nc_with_keys, role_arn = 'arn:aws:iam::123456789012:role/evil', "
        f"format = 'TSV', structure = 'x UInt8') {ALLOW}"
    )
    assert error, error


def test_named_collection_key_override_drops_inherited_session_token():
    # A collection that stores temporary credentials (key pair + session_token). When a query supplies its
    # own key pair but no session_token, the collection's token must not be inherited: it was issued for the
    # collection's keys and MinIO rejects it alongside a different key pair (and it would poison the STS base
    # when a query-supplied role_arn is present). Mirrors the explicit-URL path.
    url = f"http://{cluster.minio_host}:{cluster.minio_port}/root/nc_keys_and_token/data.tsv"
    node.query(
        f"INSERT INTO FUNCTION s3('{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8') "
        "SELECT 7 SETTINGS s3_truncate_on_insert = 1"
    )

    # Control: using the collection as-is sends its stale session_token, which MinIO rejects.
    error = node.query_and_get_error(
        "SELECT * FROM s3(nc_keys_and_token, format = 'TSV', structure = 'x UInt8')"
    )
    assert "403" in error or "Access Denied" in error or "AccessDenied" in error, error

    # A query that overrides the key pair (no session_token) drops the collection's token, so the read
    # succeeds with the query's keys alone.
    assert (
        node.query(
            f"SELECT * FROM s3(nc_keys_and_token, access_key_id = '{minio_access_key}', "
            f"secret_access_key = '{minio_secret_key}', format = 'TSV', structure = 'x UInt8')"
        ).strip()
        == "7"
    )


def test_backup_key_override_drops_inherited_session_token():
    # Same as above for the backup credential path (registerBackupEngineS3 / BackupIO_S3): a query key-pair
    # override with no session_token must not inherit the collection's stale token.
    node.query("DROP TABLE IF EXISTS t_backup_token_override SYNC")
    node.query("CREATE TABLE t_backup_token_override (x UInt8) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO t_backup_token_override SELECT 1")

    # Control: the collection's stale session_token is sent and MinIO rejects the backup.
    error = node.query_and_get_error(
        "BACKUP TABLE t_backup_token_override TO S3(nc_backup_keys_and_token, 'b_token_control')"
    )
    assert "403" in error or "Access Denied" in error or "AccessDenied" in error, error

    # Overriding the key pair (no session_token) drops the collection's token, so the backup succeeds.
    node.query(
        f"BACKUP TABLE t_backup_token_override TO S3(nc_backup_keys_and_token, "
        f"access_key_id = '{minio_access_key}', secret_access_key = '{minio_secret_key}', 'b_token_ok')"
    )

    node.query("DROP TABLE t_backup_token_override SYNC")


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


def test_backup_explicit_keys_drop_inherited_session_token():
    # BACKUP TO S3(url, ak, sk) with explicit keys must not send the server's inherited per-endpoint
    # session_token (the explicit form has no way to supply one). The backup succeeding proves the token was
    # dropped -- minio rejects the bogus inherited token if it is sent.
    node_with_server_session_token.query("DROP TABLE IF EXISTS t_backup_token SYNC")
    node_with_server_session_token.query(
        "CREATE TABLE t_backup_token (x UInt8) ENGINE = MergeTree ORDER BY tuple()"
    )
    node_with_server_session_token.query("INSERT INTO t_backup_token SELECT 1")

    node_with_server_session_token.query(
        f"BACKUP TABLE t_backup_token TO "
        f"S3('http://minio1:9001/root/sessiontoken/backup1', '{minio_access_key}', '{minio_secret_key}')"
    )

    node_with_server_session_token.query("DROP TABLE t_backup_token SYNC")


def test_backup_role_arn_override_does_not_use_collection_keys():
    # A query-overridden `role_arn` on a backup collection that carries operator-provisioned keys is honored
    # (STS assume-role stays allowed under the restriction), but it must not be assumed using those keys as
    # the STS base: the collection keys are dropped and the assume-role call is signed with the server's
    # ambient identity instead. Here the assume fails (there is no STS endpoint), so the backup errors - it
    # must NOT silently succeed by writing with the collection's keys.
    node.query("DROP TABLE IF EXISTS t_backup_override SYNC")
    node.query("CREATE TABLE t_backup_override (x UInt8) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO t_backup_override SELECT 1")

    error = node.query_and_get_error(
        "BACKUP TABLE t_backup_override TO "
        "S3(nc_backup_with_keys, role_arn = 'arn:aws:iam::123456789012:role/evil', 'b_override')"
    )
    assert "server-managed credentials" not in error, error
    assert error

    # Control: without the injected role the collection keys are used directly and the backup succeeds.
    node.query("BACKUP TABLE t_backup_override TO S3(nc_backup_with_keys, 'b_override_control')")

    # With the opt-in the injected role is honored with the collection keys as the STS base; the assume is
    # still attempted (and fails, there is no STS endpoint) instead of writing with the collection keys.
    error = node.query_and_get_error(
        "BACKUP TABLE t_backup_override TO "
        f"S3(nc_backup_with_keys, role_arn = 'arn:aws:iam::123456789012:role/evil', 'b_override_allowed') {ALLOW}"
    )
    assert error

    node.query("DROP TABLE t_backup_override SYNC")


def test_backup_role_only_collection_assumes_role_with_ambient_identity():
    # A backup collection that defines only a `role_arn` (no base keys) is allowed under the restriction: the
    # role is assumed with the server's ambient identity (only the assumed role's credentials would sign the
    # S3 requests). Here the assume fails (there is no STS endpoint) and the backup goes out unsigned, so it
    # errors - but it must not be the ACCESS_DENIED restriction rejection.
    node.query("DROP TABLE IF EXISTS t_backup_role_only SYNC")
    node.query("CREATE TABLE t_backup_role_only (x UInt8) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO t_backup_role_only SELECT 1")

    error = node.query_and_get_error(
        "BACKUP TABLE t_backup_role_only TO S3(nc_backup_role_only, 'b_role_only')"
    )
    assert "server-managed credentials" not in error, error
    assert "403" in error or "Access Denied" in error or "AccessDenied" in error, error

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


def test_backup_named_collection_non_overridable_key_cannot_be_redirected():
    # A backup named collection with operator-provisioned keys and a non-overridable `url` must not let a user
    # redirect the endpoint while keeping the collection's keys. The override permission is enforced like the
    # table-function/storage paths (`tryGetNamedCollectionWithOverrides`), not bypassed -- otherwise a user
    # could point the operator keys at an arbitrary endpoint.
    node.query("DROP TABLE IF EXISTS t_backup_locked SYNC")
    node.query("CREATE TABLE t_backup_locked (x UInt8) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO t_backup_locked SELECT 1")

    error = node.query_and_get_error(
        "BACKUP TABLE t_backup_locked TO "
        "S3(nc_backup_locked_url, url = 'http://minio1:9001/root/backup_redirected/', 'b1')"
    )
    assert "Override not allowed" in error and "url" in error, error

    node.query("DROP TABLE t_backup_locked SYNC")


def test_backup_explicit_keys_drop_inherited_gcp_oauth():
    # The server <s3> endpoint config carries `gcp_oauth` plus a complete Google ADC triple. A BACKUP TO
    # S3(url, ak, sk) with explicit keys must not inherit that gcp_oauth (which would mint a server bearer
    # token to the user-chosen endpoint); it must use only the explicit keys. The backup succeeding proves the
    # inherited gcp_oauth/ADC was dropped -- otherwise the GCP OAuth client is used instead of the keys and the
    # backup fails. (The earlier strip only fired when the server config had no complete ADC triple.)
    node_with_server_gcp_oauth.query("DROP TABLE IF EXISTS t_backup_gcp_inherit SYNC")
    node_with_server_gcp_oauth.query(
        "CREATE TABLE t_backup_gcp_inherit (x UInt8) ENGINE = MergeTree ORDER BY tuple()"
    )
    node_with_server_gcp_oauth.query("INSERT INTO t_backup_gcp_inherit SELECT 1")

    node_with_server_gcp_oauth.query(
        f"BACKUP TABLE t_backup_gcp_inherit TO "
        f"S3('http://minio1:9001/root/gcpoauth/backup1', '{minio_access_key}', '{minio_secret_key}')"
    )

    node_with_server_gcp_oauth.query("DROP TABLE t_backup_gcp_inherit SYNC")


def test_bare_url_s3_does_not_inherit_server_sse():
    # The bare-URL `s3('url', key, secret)` form cannot supply request-auth material, so the server <s3>
    # endpoint SSE-C key must not be inherited (it would be sent to the user-chosen endpoint). Reading a
    # plaintext object with explicit keys must succeed; an inherited SSE-C key makes the GET fail.
    url = "http://minio1:9001/root/sse_test/bare_url.tsv"
    node.query(
        f"INSERT INTO FUNCTION s3('{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8') "
        "SELECT 13 SETTINGS s3_truncate_on_insert = 1"
    )
    assert (
        node_with_server_sse.query(
            f"SELECT * FROM s3('{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8')"
        ).strip()
        == "13"
    )


def test_backup_named_collection_does_not_inherit_server_sse():
    # A backup named collection must not inherit the server <s3> SSE-C key: the backup objects are written
    # without it, so the backup metadata object can be read back with plain keys. If the server SSE-C key leaked
    # into the backup client, the objects would be encrypted with it and a plain GET would fail.
    node_with_server_sse.query("DROP TABLE IF EXISTS t_backup_sse SYNC")
    node_with_server_sse.query(
        "CREATE TABLE t_backup_sse (x UInt8) ENGINE = MergeTree ORDER BY tuple()"
    )
    node_with_server_sse.query("INSERT INTO t_backup_sse SELECT 1")
    node_with_server_sse.query("BACKUP TABLE t_backup_sse TO S3(nc_backup_sse, 'b1')")

    # Read the backup metadata object back with plain keys (no SSE-C headers): succeeds only if it was written
    # without the server SSE-C key.
    node_with_server_sse.query(
        "SELECT count() FROM s3('http://minio1:9001/root/sse_test/backup_pv1/b1/.backup', "
        f"'{minio_access_key}', '{minio_secret_key}', 'RawBLOB')"
    )

    node_with_server_sse.query("DROP TABLE t_backup_sse SYNC")


def test_backup_explicit_keys_do_not_inherit_server_sse():
    # The explicit-key BACKUP TO S3(url, ak, sk) form (no named collection) must not inherit the server <s3>
    # SSE-C key either: the backup objects are written without it and can be read back with plain keys. If the
    # server SSE-C key leaked into the backup client, the objects would be encrypted and a plain GET would fail.
    node_with_server_sse.query("DROP TABLE IF EXISTS t_backup_n1 SYNC")
    node_with_server_sse.query(
        "CREATE TABLE t_backup_n1 (x UInt8) ENGINE = MergeTree ORDER BY tuple()"
    )
    node_with_server_sse.query("INSERT INTO t_backup_n1 SELECT 1")
    node_with_server_sse.query(
        f"BACKUP TABLE t_backup_n1 TO S3('http://minio1:9001/root/sse_test/backup_n1/b1', "
        f"'{minio_access_key}', '{minio_secret_key}')"
    )
    node_with_server_sse.query(
        "SELECT count() FROM s3('http://minio1:9001/root/sse_test/backup_n1/b1/.backup', "
        f"'{minio_access_key}', '{minio_secret_key}', 'RawBLOB')"
    )
    node_with_server_sse.query("DROP TABLE t_backup_n1 SYNC")


def test_dynamic_disk_include_locations_child_nosign_gcp_oauth_restricted():
    # A `locations.<name>` child that resolves NOSIGN is anonymous for AWS signing, but if it also resolves
    # http_client = gcp_oauth the GCP OAuth client still mints and sends the server metadata token. Such a child
    # must be restricted, not treated as anonymous because of NOSIGN.
    node_with_includes.query("DROP TABLE IF EXISTS t_loc_gcp SYNC")
    error = node_with_includes.query_and_get_error(
        "CREATE TABLE t_loc_gcp (x UInt8) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = disk(type = object_storage, object_storage_type = local_blob_storage, "
        "include = 'evil_locations_nosign_gcp')",
        settings={"dynamic_disk_allow_include": 1},
    )
    assert "ACCESS_DENIED" in error or "must provide its credentials explicitly" in error, error


def test_dynamic_disk_include_locations_child_keys_not_vouched_by_root():
    # A dummy explicit key pair on the disk root must not vouch for a `locations.<name>` S3 child whose
    # credentials were resolved from the include/server config: the child's keys are not user-supplied SQL for
    # that child. The post-include check validates credentials per resolved S3 backend, not from a root wrapper.
    node_with_includes.query("DROP TABLE IF EXISTS t_loc_keys SYNC")
    error = node_with_includes.query_and_get_error(
        "CREATE TABLE t_loc_keys (x UInt8) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = disk(type = object_storage, object_storage_type = local_blob_storage, "
        "access_key_id = 'dummy_root', secret_access_key = 'dummy_root', include = 'evil_locations_with_keys')",
        settings={"dynamic_disk_allow_include": 1},
    )
    assert "ACCESS_DENIED" in error or "must provide its credentials explicitly" in error, error


def test_dynamic_disk_include_locations_child_is_restricted():
    # An `include` can inject a `locations.<name>` child object storage. A disk root that proves non-S3
    # (object_storage with local_blob_storage) bypasses the pre-resolution check, so the post-`include`
    # re-check must inspect each `locations.*` child and reject an S3 child that resolves server credentials --
    # otherwise the child is built with `for_disk_s3 = true`, bypassing the central restriction.
    node_with_includes.query("DROP TABLE IF EXISTS t_loc SYNC")
    error = node_with_includes.query_and_get_error(
        "CREATE TABLE t_loc (x UInt8) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = disk(type = object_storage, object_storage_type = local_blob_storage, "
        "include = 'evil_locations')",
        settings={"dynamic_disk_allow_include": 1},
    )
    assert "ACCESS_DENIED" in error or "must provide its credentials explicitly" in error, error


def test_dynamic_disk_include_locations_child_local_path_is_fenced():
    # The same shape, for the local filesystem rather than S3: an `include` can inject a
    # `locations.<name>` child whose backend is `local` and whose `path` is outside
    # `custom_local_disks_base_directory`. A disk root that names no local path of its own must not let
    # that child through -- `RegisterDiskObjectStorage` builds one object storage per child, each with
    # its own `path`.
    # A `type = object_storage` disk with an `include` and no literal `object_storage_type` may resolve to S3,
    # so the credentials restriction rejects it before the disk is created; lift it to reach the path check.
    node_with_includes.query("DROP TABLE IF EXISTS t_loc_local SYNC")
    error = node_with_includes.query_and_get_error(
        "CREATE TABLE t_loc_local (x UInt8) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = disk(type = object_storage, include = 'evil_locations_local_path')",
        settings={
            "dynamic_disk_allow_include": 1,
            "s3_allow_server_credentials_in_user_queries": 1,
        },
    )
    assert "must be inside" in error, error
    assert (
        node_with_includes.query(
            "SELECT count() FROM system.disks WHERE path = '/etc/clickhouse-server/'"
        ).strip()
        == "0"
    )


def test_persistent_table_does_not_reuse_credentialed_client_across_sessions():
    # A persistent S3 table whose credentials are server-managed (use_environment_credentials) is non-static, so
    # its client is rebuilt per query. An opt-in session reading it builds a credentialed client in the shared
    # object storage; a later restricted session must NOT reuse that client. The restriction mode is part of the
    # client's identity, so the restricted session rebuilds an (anonymous) client and is still refused.
    url = "http://minio1:9001/root/persistent_env/data.tsv"
    node.query(
        f"INSERT INTO FUNCTION s3('{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8') "
        "SELECT 7 SETTINGS s3_truncate_on_insert = 1"
    )
    node.query("DROP TABLE IF EXISTS t_persistent_env SYNC")
    # The S3 engine resolves credentials at CREATE, so create it with the opt-in (the operator owns the table).
    node.query(
        "CREATE TABLE t_persistent_env (x UInt8) ENGINE = S3(nc_persistent_env)",
        settings={"s3_allow_server_credentials_in_user_queries": 1},
    )

    # Opt-in: succeeds, building a credentialed client in the shared object storage.
    assert node.query(f"SELECT * FROM t_persistent_env {ALLOW}").strip() == "7"
    # Restricted (default): must be refused -- the credentialed client primed by the opt-in session above is not
    # reused across the restriction-mode change.
    assert "ACCESS_DENIED" in node.query_and_get_error("SELECT * FROM t_persistent_env")
    # Opt-in again: still works (the restricted query did not poison the storage either).
    assert node.query(f"SELECT * FROM t_persistent_env {ALLOW}").strip() == "7"

    node.query("DROP TABLE t_persistent_env SYNC")


def test_persistent_table_created_with_opt_in_refuses_first_restricted_read():
    # A persistent S3 table created with the opt-in resolves the server credentials at create time. The FIRST
    # read in a default restricted session must still be refused -- the create-time client is not reused across
    # the restriction-mode change (the create-time settings update establishes the mode, so the restricted read
    # rebuilds anonymous and is refused).
    node.query("DROP TABLE IF EXISTS t_persist_optin SYNC")
    node.query(
        "CREATE TABLE t_persist_optin (x UInt8) ENGINE = S3(nc_persistent_env)",
        settings={"s3_allow_server_credentials_in_user_queries": 1},
    )
    assert "ACCESS_DENIED" in node.query_and_get_error("SELECT * FROM t_persist_optin")
    node.query("DROP TABLE t_persist_optin SYNC")


def test_named_collection_does_not_inherit_server_sse():
    # The server <s3> endpoint config carries an SSE-C customer key. A named collection with its own explicit
    # keys must not inherit that server-managed key (which getClient would send to the endpoint): reading a
    # plaintext object via the collection must succeed. If the SSE-C key leaks in, the GET sends SSE-C headers
    # for an object that is not SSE-C encrypted and S3 rejects it -- and the server's key would reach the
    # collection's endpoint.
    url = "http://minio1:9001/root/sse_test/data.tsv"
    # Write a plaintext object using explicit keys from a node without the SSE config.
    node.query(
        f"INSERT INTO FUNCTION s3('{url}', '{minio_access_key}', '{minio_secret_key}', 'TSV', 'x UInt8') "
        "SELECT 11 SETTINGS s3_truncate_on_insert = 1"
    )

    assert node_with_server_sse.query("SELECT * FROM s3(nc_sse_keys)").strip() == "11"


def test_server_data_disk_unaffected():
    # The server's own S3-backed operations are not user queries and keep working regardless.
    node.query("DROP TABLE IF EXISTS t_local SYNC")
    node.query("CREATE TABLE t_local (x UInt8) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO t_local SELECT 4")
    assert node.query("SELECT sum(x) FROM t_local").strip() == "4"
    node.query("DROP TABLE t_local SYNC")


@pytest.fixture(scope="module")
def bearer_catalog_server():
    start_mock_servers(
        cluster,
        os.path.dirname(__file__),
        [("bearer_token_server.py", "node", BEARER_TOKEN_PORT)],
    )
    yield f"http://{node.ip_address}:{BEARER_TOKEN_PORT}"


@pytest.fixture
def bearer_catalog(bearer_catalog_server):
    def reset(**options):
        response = requests.post(f"{bearer_catalog_server}/reset", json=options, timeout=10)
        response.raise_for_status()

    reset()
    yield reset
    node.query("DROP DATABASE IF EXISTS bearer_rest_auth")


def bearer_catalog_events(server):
    response = requests.get(f"{server}/events", timeout=10)
    response.raise_for_status()
    return response.json()


def create_bearer_catalog(extra="", allow=True, host="127.0.0.1"):
    return node.query(
        f"CREATE DATABASE bearer_rest_auth ENGINE = DataLakeCatalog('http://127.0.0.1:{BEARER_TOKEN_PORT}') "
        "SETTINGS catalog_type = 'rest', warehouse = 'test', "
        f"bearer_token_endpoint = 'http://{host}:{BEARER_TOKEN_PORT}/token?audience=https%3A%2F%2Fcatalog.example', "
        f"bearer_token_header = 'X-Token-Request: test'{extra}",
        settings={
            "allow_database_iceberg": 1,
            "s3_allow_server_credentials_in_user_queries": int(allow),
        },
    )


@pytest.mark.parametrize(
    "response_format, field",
    [("raw", None), ("json", "access_token"), ("json", "data.token")],
)
def test_rest_catalog_jwt_endpoint(
    bearer_catalog_server, bearer_catalog, response_format, field
):
    options = {"jwt_json_field": field} if response_format == "json" else {}
    bearer_catalog(**options)
    extra = f", bearer_token_response_format = '{response_format}'"
    if field == "data.token":
        extra += ", bearer_token_response_field = 'data.token'"
    create_bearer_catalog(extra)
    assert node.query("SHOW TABLES FROM bearer_rest_auth") == ""
    events = bearer_catalog_events(bearer_catalog_server)
    assert events[0]["path"] == "/token"
    assert events[0]["token_request_header"] == "test"
    assert events[0]["query"] == "audience=https%3A%2F%2Fcatalog.example"
    assert sum(event["path"] == "/token" for event in events) == 1
    catalog_requests = [event for event in events if event["path"].startswith("/v1/")]
    assert catalog_requests[0]["path"] == "/v1/config"
    assert catalog_requests[0]["authorization"].startswith("Bearer eyJ")
    assert all(
        event["authorization"] == catalog_requests[0]["authorization"]
        for event in catalog_requests
    )
    assert all(event["token_request_header"] == "" for event in catalog_requests)
    definition = node.query("SHOW CREATE DATABASE bearer_rest_auth")
    assert "catalog.example" not in definition
    assert "X-Token-Request" not in definition


@pytest.mark.parametrize("status", [401, 403])
def test_rest_catalog_jwt_rejection_refresh(bearer_catalog_server, bearer_catalog, status):
    bearer_catalog(reject_jwt_once=status, jwt_json_field="data.token")
    create_bearer_catalog(
        ", bearer_token_response_format = 'json', bearer_token_response_field = 'data.token'"
    )
    assert node.query("SHOW TABLES FROM bearer_rest_auth") == ""
    events = bearer_catalog_events(bearer_catalog_server)
    assert sum(event["path"] == "/token" for event in events) == 2
    namespace_requests = [event for event in events if event["path"] == "/v1/namespaces"]
    assert len(namespace_requests) >= 2
    assert namespace_requests[0]["authorization"] != namespace_requests[1]["authorization"]
    assert all(
        event["authorization"] == namespace_requests[1]["authorization"]
        for event in namespace_requests[1:]
    )


def test_rest_catalog_jwt_expiry_refresh(bearer_catalog_server, bearer_catalog):
    bearer_catalog(jwt_lifetime=3)
    create_bearer_catalog()

    def refreshed(_):
        events = bearer_catalog_events(bearer_catalog_server)
        return str(sum(event["path"] == "/token" for event in events) >= 2)

    assert_eq_with_retry(
        node, "SHOW TABLES FROM bearer_rest_auth", "True", get_result=refreshed
    )


@pytest.mark.parametrize(
    "options, message",
    [
        ({"jwt_body": "opaque-token"}, "three nonempty segments"),
        ({"jwt_body": "eyJhbGciOiJSUzI1NiJ9.e30.signature"}, "integer `exp` claim"),
        ({"jwt_lifetime": -1}, "expired"),
        ({"jwt_status": 500}, "HTTP status 500"),
        ({"jwt_status": 302}, "HTTP status 302"),
    ],
)
def test_rest_catalog_jwt_endpoint_failure(
    bearer_catalog_server, bearer_catalog, options, message
):
    bearer_catalog(**options)
    with pytest.raises(QueryRuntimeException, match=message):
        create_bearer_catalog()
    events = bearer_catalog_events(bearer_catalog_server)
    assert [event["path"] for event in events] == ["/token"]


def test_rest_catalog_jwt_endpoint_restrictions(bearer_catalog_server, bearer_catalog):
    bearer_catalog()
    with pytest.raises(QueryRuntimeException, match="ACCESS_DENIED"):
        create_bearer_catalog(allow=False)
    with pytest.raises(QueryRuntimeException, match="iceberg_bearer_token_host"):
        create_bearer_catalog(host="localhost")
    with pytest.raises(QueryRuntimeException, match="cannot be combined"):
        create_bearer_catalog(", auth_header = 'Authorization: Bearer explicit-token'")
    assert bearer_catalog_events(bearer_catalog_server) == []


@pytest.mark.parametrize(
    "body",
    [
        "not-json-SECRET_RESPONSE",
        '{"data": {}}',
        '{"data": {"token": 42}}',
        '{"data": {"token": null}}',
    ],
)
def test_rest_catalog_bearer_token_json_failure(
    bearer_catalog_server, bearer_catalog, body
):
    bearer_catalog(jwt_response_body=body)
    with pytest.raises(QueryRuntimeException, match="JSON object with a string") as error:
        create_bearer_catalog(
            ", bearer_token_response_format = 'json', bearer_token_response_field = 'data.token'"
        )
    assert "SECRET_RESPONSE" not in str(error.value)
    assert [event["path"] for event in bearer_catalog_events(bearer_catalog_server)] == [
        "/token"
    ]


@pytest.mark.parametrize(
    "extra, message",
    [
        (", bearer_token_response_format = 'xml'", "must be 'raw' or 'json'"),
        (
            ", bearer_token_response_format = 'json', bearer_token_response_field = ''",
            "nonempty dot-separated field path",
        ),
    ],
)
def test_rest_catalog_bearer_token_response_settings(
    bearer_catalog_server, bearer_catalog, extra, message
):
    with pytest.raises(QueryRuntimeException, match=message):
        create_bearer_catalog(extra)
    assert bearer_catalog_events(bearer_catalog_server) == []
