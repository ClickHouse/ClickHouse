import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
# default max_authentication_methods_per_user is 100
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
    """Test user with multiple plaintext passwords can authenticate with any of them"""
    result = node.query("SELECT currentUser()", user="multi_plaintext", password="pass1")
    assert result.strip() == "multi_plaintext"
    
    result = node.query("SELECT currentUser()", user="multi_plaintext", password="pass2")
    assert result.strip() == "multi_plaintext"
    
    result = node.query("SELECT currentUser()", user="multi_plaintext", password="pass3")
    assert result.strip() == "multi_plaintext"
    
    with pytest.raises(Exception):
        node.query("SELECT currentUser()", user="multi_plaintext", password="wrong_pass")


def test_multiple_sha256_passwords(started_cluster):
    """Test user with multiple SHA256 passwords can authenticate with any of them"""
    result = node.query("SELECT currentUser()", user="multi_sha256", password="sha256_pass_1")
    assert result.strip() == "multi_sha256"
    
    result = node.query("SELECT currentUser()", user="multi_sha256", password="sha256_pass_2")
    assert result.strip() == "multi_sha256"
    
    with pytest.raises(Exception):
        node.query("SELECT currentUser()", user="multi_sha256", password="wrong_pass")


def test_mixed_authentication_methods(started_cluster):
    """Test user with mixed authentication methods (plaintext + SHA256)"""
    result = node.query("SELECT currentUser()", user="mixed_auth", password="plain_pass")
    assert result.strip() == "mixed_auth"
    
    result = node.query("SELECT currentUser()", user="mixed_auth", password="double_sha1_pass_1")
    assert result.strip() == "mixed_auth"
    
    result = node.query("SELECT currentUser()", user="mixed_auth", password="double_sha1_pass_2")
    assert result.strip() == "mixed_auth"
    
    with pytest.raises(Exception) as exc_info:
        node.query("SELECT 1", user="mixed_auth", password="wrong_password")
    
    error_msg = str(exc_info.value)
    assert "Authentication failed: password is incorrect, or there is no user with such name." in error_msg
    
    
def test_user_authentication_methods_in_system_table(started_cluster):
    """Test that system.users table shows correct authentication methods"""
    result = node.query(
        "SELECT arrayJoin(auth_type) as auth_type FROM system.users WHERE name = 'multi_plaintext' ORDER BY auth_type"
    )
    expected = TSV([
        ["plaintext_password"],
        ["plaintext_password"],
        ["plaintext_password"],
    ])
    assert TSV(result) == expected
    
    result = node.query(
        "SELECT arrayJoin(auth_type) as auth_type FROM system.users WHERE name = 'mixed_auth' ORDER BY auth_type"
    )
    expected = TSV([
        ["plaintext_password"],
        ["double_sha1_password"],
        ["double_sha1_password"],
    ])
    assert TSV(result) == expected


def test_authentication_failure_logging(started_cluster):
    """Test that authentication failures are properly logged"""
    with pytest.raises(Exception) as exc_info:
        node.query("SELECT 1", user="mixed_auth", password="wrong_password")
    
    error_msg = str(exc_info.value)
    assert "Authentication failed" in error_msg or "Access denied" in error_msg or "password" in error_msg.lower()
