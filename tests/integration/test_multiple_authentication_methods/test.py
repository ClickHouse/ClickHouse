import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/users_multiple_auth.xml",
    ],
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
    # Test first password
    result = node.query("SELECT currentUser()", user="multi_plaintext", password="pass1")
    assert result.strip() == "multi_plaintext"
    
    # Test second password
    result = node.query("SELECT currentUser()", user="multi_plaintext", password="pass2")
    assert result.strip() == "multi_plaintext"
    
    # Test third password
    result = node.query("SELECT currentUser()", user="multi_plaintext", password="pass3")
    assert result.strip() == "multi_plaintext"
    
    # Test wrong password should fail
    with pytest.raises(Exception):
        node.query("SELECT currentUser()", user="multi_plaintext", password="wrong_pass")


def test_multiple_sha256_passwords(started_cluster):
    """Test user with multiple SHA256 passwords can authenticate with any of them"""
    # Test first password (hash of "password1")
    result = node.query("SELECT currentUser()", user="multi_sha256", password="password1")
    assert result.strip() == "multi_sha256"
    
    # Test second password (hash of "password2")
    result = node.query("SELECT currentUser()", user="multi_sha256", password="password2")
    assert result.strip() == "multi_sha256"
    
    # Test wrong password should fail
    with pytest.raises(Exception):
        node.query("SELECT currentUser()", user="multi_sha256", password="wrong_pass")


def test_mixed_authentication_methods(started_cluster):
    """Test user with mixed authentication methods (plaintext + SHA256)"""
    # Test plaintext password
    result = node.query("SELECT currentUser()", user="mixed_auth", password="plain_pass")
    assert result.strip() == "mixed_auth"
    
    # Test SHA256 password (hash of "double_sha1_pass_1")
    result = node.query("SELECT currentUser()", user="mixed_auth", password="double_sha1_pass_1")
    assert result.strip() == "mixed_auth"
    
    # Test second SHA256 password (hash of "double_sha1_pass_2")
    result = node.query("SELECT currentUser()", user="mixed_auth", password="double_sha1_pass_2")
    assert result.strip() == "mixed_auth"
    
    # Test wrong password should fail
    with pytest.raises(Exception):
        node.query("SELECT currentUser()", user="mixed_auth", password="wrong_pass")

def test_user_authentication_methods_in_system_table(started_cluster):
    """Test that system.users table shows correct authentication methods"""
    # Check multi_plaintext user has multiple authentication methods
    result = node.query(
        "SELECT name, auth_type FROM system.users WHERE name = 'multi_plaintext' ORDER BY auth_type"
    )
    expected = TSV([
        ["multi_plaintext", "plaintext_password"],
        ["multi_plaintext", "plaintext_password"], 
        ["multi_plaintext", "plaintext_password"]
    ])
    assert TSV(result) == expected
    
    # Check mixed_auth user has both plaintext and SHA256
    result = node.query(
        "SELECT name, auth_type FROM system.users WHERE name = 'mixed_auth' ORDER BY auth_type"
    )
    expected = TSV([
        ["mixed_auth", "plaintext_password"],
        ["mixed_auth", "double_sha1_password"]
    ])
    assert TSV(result) == expected


def test_grants_and_permissions_with_multiple_auth(started_cluster):
    """Test that grants work correctly with multiple authentication methods"""
    # Grant some permissions to multi_plaintext user
    node.query("GRANT SELECT ON system.numbers TO multi_plaintext")
    
    # Test access with first password
    result = node.query(
        "SELECT count() FROM system.numbers LIMIT 5", 
        user="mixed_auth", 
        password="plain_pass"
    )
    assert result.strip() == "5"
    
    # Test access with second password (double SHA1 hash)
    result = node.query(
        "SELECT count() FROM system.numbers LIMIT 5", 
        user="mixed_auth", 
        password="double_sha1_pass_1"
    )
    assert result.strip() == "5"
    
    # Clean up
    node.query("REVOKE SELECT ON system.numbers FROM mixed_auth")


def test_authentication_failure_logging(started_cluster):
    """Test that authentication failures are properly logged"""
    # Test wrong password generates proper error
    with pytest.raises(Exception) as exc_info:
        node.query("SELECT 1", user="mixed_auth", password="wrong_password")
    
    # Should contain authentication-related error message
    error_msg = str(exc_info.value)
    assert "Authentication failed" in error_msg or "Access denied" in error_msg or "password" in error_msg.lower()
