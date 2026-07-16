import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=[
        "configs/users_multiple_auth.xml",
    ],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_multiple_plaintext_passwords(started_cluster):
    """User with multiple plaintext passwords can authenticate with any of them"""
    for password in ["pass1", "pass2", "pass3"]:
        result = node.query("SELECT currentUser()", user="multi_plaintext", password=password)
        assert result.strip() == "multi_plaintext"

    with pytest.raises(Exception):
        node.query("SELECT currentUser()", user="multi_plaintext", password="wrong_pass")


def test_multiple_sha256_passwords(started_cluster):
    """User with multiple SHA256 passwords can authenticate with any of them"""
    result = node.query("SELECT currentUser()", user="multi_sha256", password="sha256_pass_1")
    assert result.strip() == "multi_sha256"

    result = node.query("SELECT currentUser()", user="multi_sha256", password="sha256_pass_2")
    assert result.strip() == "multi_sha256"

    with pytest.raises(Exception):
        node.query("SELECT currentUser()", user="multi_sha256", password="wrong_pass")


def test_mixed_authentication_methods(started_cluster):
    """User with mixed auth types (plaintext + double_sha1) can authenticate with any of them"""
    result = node.query("SELECT currentUser()", user="mixed_auth", password="plain_pass")
    assert result.strip() == "mixed_auth"

    result = node.query("SELECT currentUser()", user="mixed_auth", password="double_sha1_pass_1")
    assert result.strip() == "mixed_auth"

    result = node.query("SELECT currentUser()", user="mixed_auth", password="double_sha1_pass_2")
    assert result.strip() == "mixed_auth"

    with pytest.raises(Exception):
        node.query("SELECT currentUser()", user="mixed_auth", password="wrong_pass")


def test_auth_types_in_system_table(started_cluster):
    """system.users reflects the correct auth types for all users.

    auth_type is Enum8, so ORDER BY auth_type sorts by numeric enum value, not
    alphabetically by name. The expected values below must match that ordering:
      plaintext_password = 1, double_sha1_password = 3, sha256_password = 2.
    """
    result = node.query(
        "SELECT arrayJoin(arraySort(auth_type)) AS auth_type FROM system.users WHERE name = 'multi_plaintext'"
    )
    assert TSV(result) == TSV([["plaintext_password"]] * 3)

    result = node.query(
        "SELECT arrayJoin(arraySort(auth_type)) AS auth_type FROM system.users WHERE name = 'multi_sha256'"
    )
    assert TSV(result) == TSV([["sha256_password"]] * 2)

    result = node.query(
        "SELECT arrayJoin(arraySort(auth_type)) AS auth_type FROM system.users WHERE name = 'mixed_auth'"
    )
    # plaintext_password(1) < double_sha1_password(3), so plaintext comes first
    assert TSV(result) == TSV([["plaintext_password"], ["double_sha1_password"], ["double_sha1_password"]])
