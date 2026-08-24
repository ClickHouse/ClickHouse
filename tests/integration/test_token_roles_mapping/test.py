"""
Smoke tests for the `<roles_mapping>` stage in TokenAccessStorage.

The mapping rewrites incoming group identifiers (e.g. Entra security-group object IDs)
to ClickHouse role names BEFORE `roles_filter` and `roles_transform` run. The processor
under test is `jwt_static_key` with HS256 so tokens can be crafted inline without an IdP.

Run:
    pytest tests/integration/test_token_roles_mapping/test.py -v
"""

import jwt
import pytest

from helpers.cluster import ClickHouseCluster

SECRET = "roles_mapping_test_secret"

GUID_ADMIN = "8a1b2c3d-4e5f-6789-abcd-ef0123456789"
GUID_ANALYST = "9f8e7d6c-5b4a-3210-fedc-ba0987654321"
GUID_UNMAPPED = "11111111-2222-3333-4444-555555555555"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/validators.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        node.query("DROP ROLE IF EXISTS ch_admin")
        node.query("DROP ROLE IF EXISTS ch_analyst")
        node.query("CREATE ROLE ch_admin")
        node.query("CREATE ROLE ch_analyst")
        yield cluster
    finally:
        cluster.shutdown()


def make_jwt(sub, groups):
    return jwt.encode({"sub": sub, "groups": groups}, SECRET, algorithm="HS256")


def query_with_token(token, sql):
    resp = node.http_request(
        "",
        method="POST",
        data=sql,
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    return resp.text


def current_roles(sub, groups):
    token = make_jwt(sub, groups)
    raw = query_with_token(
        token,
        "SELECT role_name FROM system.current_roles ORDER BY role_name FORMAT TabSeparated",
    )
    return [line for line in raw.splitlines() if line]


def test_mapped_guid_grants_mapped_role():
    """A GUID listed in <roles_mapping> resolves to the mapped ClickHouse role."""
    assert current_roles("alice", [GUID_ADMIN]) == ["ch_admin"]


def test_multiple_guids_grant_multiple_roles():
    assert current_roles("bob", [GUID_ADMIN, GUID_ANALYST]) == ["ch_admin", "ch_analyst"]


def test_unmapped_guid_is_dropped_by_filter():
    """An unmapped GUID passes through `roles_mapping` unchanged and is then rejected by
    `roles_filter` (^ch_[a-z_]+$ doesn't match a raw GUID), so only the mapped role survives."""
    assert current_roles("charlie", [GUID_ADMIN, GUID_UNMAPPED]) == ["ch_admin"]


def test_only_unmapped_guids_yield_no_roles():
    """No GUID is in the mapping and the filter rejects all of them: no roles are granted,
    but authentication itself still succeeds and the user is created from the token."""
    assert current_roles("dave", [GUID_UNMAPPED]) == []
    token = make_jwt("dave", [GUID_UNMAPPED])
    assert query_with_token(token, "SELECT currentUser() FORMAT TabSeparated").strip() == "dave"
