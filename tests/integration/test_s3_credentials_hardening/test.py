"""Verify that user SQL cannot reuse server-managed `<s3>` config credentials.

The cluster instance has top-level `<s3><access_key_id>` and
`<s3><secret_access_key>` set in its main config. Before the hardening fix
in `S3StorageParsedArguments`, supplying a user-side `role_arn` (via the
`s3()` table function, a named collection, or `BACKUP TO S3`) without
matching user-supplied keys would silently pick up the admin's keys to sign
the STS request. After the fix, every such call is rejected with
`ACCESS_DENIED` at parse time.

Most checks fire before any network call. The omission checks use a small mock
endpoint to verify that credential-less user SQL does not inherit top-level
server credentials or environment credentials.
"""

import os

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/admin_s3.xml"],
    env_variables={
        "AWS_ACCESS_KEY_ID": "ENV_FAKE_KEY",
        "AWS_SECRET_ACCESS_KEY": "ENV_FAKE_SECRET",
    },
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
        start_mock_servers(cluster, script_dir, [("credential_echo.py", "resolver", "18080")])
        yield cluster
    finally:
        cluster.shutdown()


ROLE_ARN = "arn:aws:iam::123456789012:role/Test"
ENDPOINT = "http://unreachable.invalid/bucket/object.tsv"
UNTRUSTED_ENDPOINT = "http://resolver:18080/untrusted/object.csv"
TRUSTED_ENDPOINT = "http://resolver:18080/trusted/object.csv"
RELOAD_TRUSTED_ENDPOINT = "http://resolver:18080/reload_trusted/object.csv"
RELOAD_TRUSTED_ENDPOINT_CONFIG = """        <reload_trusted_endpoint>
            <endpoint>http://resolver:18080/reload_trusted/</endpoint>
            <access_key_id>TRUSTED_FAKE_KEY</access_key_id>
            <secret_access_key>TRUSTED_FAKE_SECRET</secret_access_key>
            <header>X-Admin-Secret: TRUSTED_HEADER</header>
        </reload_trusted_endpoint>
"""
RELOAD_TRUSTED_ENDPOINT_WITHOUT_CREDENTIALS_CONFIG = """        <reload_trusted_endpoint>
            <endpoint>http://resolver:18080/reload_trusted/</endpoint>
        </reload_trusted_endpoint>
"""


def _set_reload_trusted_endpoint(enabled, with_credentials=True):
    config_path = os.path.join(
        os.path.dirname(__file__),
        cluster.instances_dir_name,
        "node/configs/config.d/admin_s3.xml",
    )
    with open(config_path, encoding="utf-8") as config:
        contents = config.read()

    contents = contents.replace(RELOAD_TRUSTED_ENDPOINT_CONFIG, "")
    contents = contents.replace(RELOAD_TRUSTED_ENDPOINT_WITHOUT_CREDENTIALS_CONFIG, "")
    if enabled:
        endpoint_config = (
            RELOAD_TRUSTED_ENDPOINT_CONFIG
            if with_credentials
            else RELOAD_TRUSTED_ENDPOINT_WITHOUT_CREDENTIALS_CONFIG
        )
        contents = contents.replace(
            "    </s3>\n", endpoint_config + "    </s3>\n"
        )

    with open(config_path, "w", encoding="utf-8") as config:
        config.write(contents)


def _expect_access_denied(query):
    with pytest.raises(QueryRuntimeException) as exc_info:
        node.query(query)
    assert "ACCESS_DENIED" in str(exc_info.value), str(exc_info.value)


def test_s3_table_function_role_arn_does_not_inherit_admin_keys():
    # No user-supplied keys; admin <s3> config has both. Must be rejected.
    _expect_access_denied(
        f"""
        SELECT *
        FROM s3('{ENDPOINT}', format = 'TSV', structure = 'x UInt8',
                extra_credentials(role_arn = '{ROLE_ARN}'))
        """
    )


def test_s3_table_function_role_arn_with_partial_user_keys():
    # User supplies only access_key_id, leaves secret empty. The admin config
    # has a secret, but the check ignores it because it was not user-supplied.
    _expect_access_denied(
        f"""
        SELECT *
        FROM s3('{ENDPOINT}', 'user_key', '',
                format = 'TSV', structure = 'x UInt8',
                extra_credentials(role_arn = '{ROLE_ARN}'))
        """
    )


def test_s3_table_function_omission_does_not_inherit_server_credentials():
    result = node.query(
        f"SELECT * FROM s3('{UNTRUSTED_ENDPOINT}', 'CSV', 'leaked UInt8')"
    )
    assert result.strip() == "0"


def test_named_collection_omission_does_not_inherit_server_credentials():
    node.query("DROP NAMED COLLECTION IF EXISTS nc_omission")
    node.query(
        f"""
        CREATE NAMED COLLECTION nc_omission AS
            url = '{UNTRUSTED_ENDPOINT}'
        """
    )
    try:
        result = node.query(
            "SELECT * FROM s3(nc_omission, format = 'CSV', structure = 'leaked UInt8')"
        )
        assert result.strip() == "0"
    finally:
        node.query("DROP NAMED COLLECTION IF EXISTS nc_omission")


def test_s3_storage_refresh_does_not_inherit_server_headers():
    node.query("DROP TABLE IF EXISTS s3_refresh")
    node.query(
        f"""
        CREATE TABLE s3_refresh (leaked UInt8)
        ENGINE = S3('{UNTRUSTED_ENDPOINT}', 'CSV')
        """
    )
    try:
        assert node.query("SELECT * FROM s3_refresh").strip() == "0"
        node.query("SYSTEM RELOAD CONFIG")
        assert node.query("SELECT * FROM s3_refresh").strip() == "0"
    finally:
        node.query("DROP TABLE IF EXISTS s3_refresh")


def test_s3_storage_refresh_drops_revoked_endpoint_credentials():
    node.query("DROP TABLE IF EXISTS s3_refresh_revoked_endpoint")
    node.query("DROP TABLE IF EXISTS s3_refresh_initial_endpoint")
    node.query("DROP TABLE IF EXISTS s3_refresh_user_credentials")
    node.query("DROP TABLE IF EXISTS s3_refresh_user_headers")
    _set_reload_trusted_endpoint(False)
    node.query("SYSTEM RELOAD CONFIG")
    node.query(
        f"""
        CREATE TABLE s3_refresh_revoked_endpoint (leaked UInt8)
        ENGINE = S3('{RELOAD_TRUSTED_ENDPOINT}', 'CSV')
        """
    )
    try:
        assert node.query("SELECT * FROM s3_refresh_revoked_endpoint").strip() == "0"

        _set_reload_trusted_endpoint(True)
        node.query("SYSTEM RELOAD CONFIG")
        assert node.query("SELECT * FROM s3_refresh_revoked_endpoint").strip() == "2"

        _set_reload_trusted_endpoint(True, with_credentials=False)
        node.query("SYSTEM RELOAD CONFIG")
        assert node.query("SELECT * FROM s3_refresh_revoked_endpoint").strip() == "1"

        _set_reload_trusted_endpoint(True)
        node.query("SYSTEM RELOAD CONFIG")
        assert node.query("SELECT * FROM s3_refresh_revoked_endpoint").strip() == "2"

        _set_reload_trusted_endpoint(False)
        node.query("SYSTEM RELOAD CONFIG")
        assert node.query("SELECT * FROM s3_refresh_revoked_endpoint").strip() == "0"

        _set_reload_trusted_endpoint(True)
        node.query("SYSTEM RELOAD CONFIG")
        node.query(
            f"""
            CREATE TABLE s3_refresh_initial_endpoint (leaked UInt8)
            ENGINE = S3('{RELOAD_TRUSTED_ENDPOINT}', 'CSV')
            """
        )
        assert node.query("SELECT * FROM s3_refresh_initial_endpoint").strip() == "2"

        _set_reload_trusted_endpoint(False)
        node.query("SYSTEM RELOAD CONFIG")
        assert node.query("SELECT * FROM s3_refresh_initial_endpoint").strip() == "0"

        _set_reload_trusted_endpoint(True)
        node.query("SYSTEM RELOAD CONFIG")
        node.query(
            f"""
            CREATE TABLE s3_refresh_user_credentials (leaked UInt8)
            ENGINE = S3('{RELOAD_TRUSTED_ENDPOINT}', 'USER_FAKE_KEY', 'USER_FAKE_SECRET', 'CSV')
            """
        )
        assert node.query("SELECT * FROM s3_refresh_user_credentials").strip() == "0"

        _set_reload_trusted_endpoint(False)
        node.query("SYSTEM RELOAD CONFIG")
        assert node.query("SELECT * FROM s3_refresh_user_credentials").strip() == "0"

        _set_reload_trusted_endpoint(True)
        node.query("SYSTEM RELOAD CONFIG")
        node.query(
            f"""
            CREATE TABLE s3_refresh_user_headers (leaked UInt8)
            ENGINE = S3('{RELOAD_TRUSTED_ENDPOINT}', 'CSV',
                        headers('X-User-Header' = 'user'))
            """
        )
        assert node.query("SELECT * FROM s3_refresh_user_headers").strip() == "2"

        _set_reload_trusted_endpoint(False)
        node.query("SYSTEM RELOAD CONFIG")
        assert node.query("SELECT * FROM s3_refresh_user_headers").strip() == "6"
    finally:
        node.query("DROP TABLE IF EXISTS s3_refresh_revoked_endpoint")
        node.query("DROP TABLE IF EXISTS s3_refresh_initial_endpoint")
        node.query("DROP TABLE IF EXISTS s3_refresh_user_credentials")
        node.query("DROP TABLE IF EXISTS s3_refresh_user_headers")
        _set_reload_trusted_endpoint(False)
        node.query("SYSTEM RELOAD CONFIG")


def test_endpoint_scoped_credentials_still_apply():
    result = node.query(
        f"SELECT * FROM s3('{TRUSTED_ENDPOINT}', 'CSV', 'leaked UInt8')"
    )
    assert result.strip() == "2"


def test_endpoint_scoped_credentials_still_apply_with_user_headers():
    result = node.query(
        f"""
        SELECT *
        FROM s3('{TRUSTED_ENDPOINT}', 'CSV', 'leaked UInt8',
                headers('X-User-Header' = 'user'))
        """
    )
    assert result.strip() == "2"


def test_endpoint_scoped_credentials_still_apply_with_no_sign_false():
    result = node.query(
        f"""
        SELECT *
        FROM s3('{TRUSTED_ENDPOINT}', format = 'CSV', structure = 'leaked UInt8',
                no_sign = 0)
        """
    )
    assert result.strip() == "2"


def test_named_collection_endpoint_scoped_credentials_still_apply():
    node.query("DROP NAMED COLLECTION IF EXISTS nc_trusted_endpoint")
    node.query(
        f"""
        CREATE NAMED COLLECTION nc_trusted_endpoint AS
            url = '{TRUSTED_ENDPOINT}'
        """
    )
    try:
        result = node.query(
            "SELECT * FROM s3(nc_trusted_endpoint, format = 'CSV', structure = 'leaked UInt8')"
        )
        assert result.strip() == "2"
    finally:
        node.query("DROP NAMED COLLECTION IF EXISTS nc_trusted_endpoint")


def test_named_collection_endpoint_scoped_credentials_still_apply_with_no_sign_false():
    node.query("DROP NAMED COLLECTION IF EXISTS nc_trusted_endpoint_no_sign_false")
    node.query(
        f"""
        CREATE NAMED COLLECTION nc_trusted_endpoint_no_sign_false AS
            url = '{TRUSTED_ENDPOINT}',
            no_sign_request = 0
        """
    )
    try:
        result = node.query(
            """
            SELECT *
            FROM s3(nc_trusted_endpoint_no_sign_false,
                    format = 'CSV', structure = 'leaked UInt8')
            """
        )
        assert result.strip() == "2"
    finally:
        node.query("DROP NAMED COLLECTION IF EXISTS nc_trusted_endpoint_no_sign_false")


def test_named_collection_role_arn_does_not_inherit_admin_keys():
    node.query("DROP NAMED COLLECTION IF EXISTS nc_role_no_keys")
    node.query(
        f"""
        CREATE NAMED COLLECTION nc_role_no_keys AS
            url = '{ENDPOINT}',
            role_arn = '{ROLE_ARN}'
        """
    )
    try:
        _expect_access_denied(
            "SELECT * FROM s3(nc_role_no_keys, format = 'TSV', structure = 'x UInt8')"
        )
    finally:
        node.query("DROP NAMED COLLECTION IF EXISTS nc_role_no_keys")


def test_named_collection_role_arn_with_partial_keys():
    # Collection supplies only one half of the key pair.
    node.query("DROP NAMED COLLECTION IF EXISTS nc_role_partial")
    node.query(
        f"""
        CREATE NAMED COLLECTION nc_role_partial AS
            url = '{ENDPOINT}',
            access_key_id = 'user_key',
            role_arn = '{ROLE_ARN}'
        """
    )
    try:
        _expect_access_denied(
            "SELECT * FROM s3(nc_role_partial, format = 'TSV', structure = 'x UInt8')"
        )
    finally:
        node.query("DROP NAMED COLLECTION IF EXISTS nc_role_partial")


def test_backup_to_s3_role_arn_does_not_inherit_admin_keys():
    node.query("DROP TABLE IF EXISTS dummy_backup_source")
    node.query("CREATE TABLE dummy_backup_source (x UInt8) ENGINE = MergeTree ORDER BY tuple()")
    try:
        _expect_access_denied(
            f"""
            BACKUP TABLE dummy_backup_source
            TO S3('{ENDPOINT}', extra_credentials(role_arn = '{ROLE_ARN}'))
            """
        )
    finally:
        node.query("DROP TABLE IF EXISTS dummy_backup_source")


def test_named_collection_with_role_arn_and_user_keys_is_allowed():
    # Sanity check: when the user supplies both keys in the same scope, the
    # check passes and the query proceeds to actual storage logic. We only
    # assert that the parse-time hardening did not fire.
    node.query("DROP NAMED COLLECTION IF EXISTS nc_role_ok")
    node.query(
        f"""
        CREATE NAMED COLLECTION nc_role_ok AS
            url = '{ENDPOINT}',
            access_key_id = 'user_key',
            secret_access_key = 'user_secret',
            role_arn = '{ROLE_ARN}'
        """
    )
    try:
        # `DESCRIBE TABLE` with an explicit `structure` exercises `fromNamedCollection`
        # without performing any S3 network call.
        result = node.query(
            "DESCRIBE TABLE s3(nc_role_ok, format = 'TSV', structure = 'x UInt8')"
        )
        assert "x" in result and "UInt8" in result
    finally:
        node.query("DROP NAMED COLLECTION IF EXISTS nc_role_ok")
