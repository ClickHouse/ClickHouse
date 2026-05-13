"""Verify that user SQL cannot reuse server-managed `<s3>` config credentials
to sign an STS `AssumeRole` flow.

The cluster instance has top-level `<s3><access_key_id>` and
`<s3><secret_access_key>` set in its main config. Before the hardening fix
in `S3StorageParsedArguments`, supplying a user-side `role_arn` (via the
`s3()` table function, a named collection, or `BACKUP TO S3`) without
matching user-supplied keys would silently pick up the admin's keys to sign
the STS request. After the fix, every such call is rejected with
`ACCESS_DENIED` at parse time.

These checks fire before any network call, so the test does not need a real
S3 endpoint.
"""

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/admin_s3.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


ROLE_ARN = "arn:aws:iam::123456789012:role/Test"
ENDPOINT = "http://unreachable.invalid/bucket/object.tsv"


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
