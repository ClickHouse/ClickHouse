import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Node without port_offset - uses default ports
node_default = cluster.add_instance(
    "node_default",
    main_configs=["configs/config.d/ports.xml"],
)

# Node with port_offset=100 - all ports shifted by 100
node_offset = cluster.add_instance(
    "node_offset",
    main_configs=[
        "configs/config.d/ports.xml",
        "configs/config.d/port_offset.xml",
    ],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_port_offset_tcp(start_cluster):
    """Test that TCP port is correctly offset"""
    # Default node should use port 9000
    result = node_default.query("SELECT 1")
    assert result.strip() == "1"
    
    # Offset node should use port 9100 (9000 + 100)
    result = node_offset.query("SELECT 1")
    assert result.strip() == "1"
    
    # Verify the actual ports being used
    default_port = node_default.query("SELECT tcpPort()").strip()
    offset_port = node_offset.query("SELECT tcpPort()").strip()
    
    assert default_port == "9000"
    assert offset_port == "9100"


def test_port_offset_http(start_cluster):
    """Test that HTTP port is correctly offset"""
    # Default node should use port 8123
    result = node_default.http_query("SELECT 1")
    assert result and result.strip() == "1"
    
    # Offset node should use port 8223 (8123 + 100)
    result = node_offset.http_query("SELECT 1")
    assert result and result.strip() == "1"
    
    # Verify using getServerPort function
    default_http_port = node_default.query("SELECT getServerPort('http_port')").strip()
    offset_http_port = node_offset.query("SELECT getServerPort('http_port')").strip()
    
    assert default_http_port == "8123"
    assert offset_http_port == "8223"


def test_port_offset_mysql(start_cluster):
    """Test that MySQL port is correctly offset"""
    # Check that MySQL ports are accessible using getServerPort
    default_port = node_default.query("SELECT getServerPort('mysql_port')").strip()
    offset_port = node_offset.query("SELECT getServerPort('mysql_port')").strip()
    
    assert default_port == "9004"
    assert offset_port == "9104"  # 9004 + 100


def test_port_offset_postgresql(start_cluster):
    """Test that PostgreSQL port is correctly offset"""
    # Check that PostgreSQL ports are accessible using getServerPort
    default_port = node_default.query("SELECT getServerPort('postgresql_port')").strip()
    offset_port = node_offset.query("SELECT getServerPort('postgresql_port')").strip()
    
    assert default_port == "9005"
    assert offset_port == "9105"  # 9005 + 100


def test_port_offset_multiple_queries(start_cluster):
    """Test that both nodes can handle queries simultaneously"""
    # Create a table on both nodes
    node_default.query("DROP TABLE IF EXISTS test_table")
    node_default.query("CREATE TABLE test_table (id UInt32) ENGINE = Memory")
    node_default.query("INSERT INTO test_table VALUES (1), (2), (3)")
    
    node_offset.query("DROP TABLE IF EXISTS test_table")
    node_offset.query("CREATE TABLE test_table (id UInt32) ENGINE = Memory")
    node_offset.query("INSERT INTO test_table VALUES (10), (20), (30)")
    
    # Query both nodes
    result_default = node_default.query("SELECT sum(id) FROM test_table").strip()
    result_offset = node_offset.query("SELECT sum(id) FROM test_table").strip()
    
    assert result_default == "6"
    assert result_offset == "60"
    
    # Cleanup
    node_default.query("DROP TABLE test_table")
    node_offset.query("DROP TABLE test_table")


def test_port_offset_system_tables(start_cluster):
    """Test that system tables reflect the correct ports"""
    # Query system.server_settings to verify port_offset is set
    offset_value = node_offset.query(
        "SELECT value FROM system.server_settings WHERE name = 'port_offset'"
    ).strip()
    assert offset_value == "100"
    
    # Default node should have port_offset = 0
    default_offset = node_default.query(
        "SELECT value FROM system.server_settings WHERE name = 'port_offset'"
    ).strip()
    assert default_offset == "0"


def test_port_offset_all_protocols(start_cluster):
    """Test that all configured ports are correctly offset"""
    # Define expected ports for default node
    expected_default_ports = {
        "tcp_port": "9000",
        "http_port": "8123",
        "mysql_port": "9004",
        "postgresql_port": "9005",
    }
    
    # Define expected ports for offset node (all +100)
    expected_offset_ports = {
        "tcp_port": "9100",
        "http_port": "8223",
        "mysql_port": "9104",
        "postgresql_port": "9105",
    }
    
    # Verify default node ports
    for port_name, expected_port in expected_default_ports.items():
        actual_port = node_default.query(f"SELECT getServerPort('{port_name}')").strip()
        assert actual_port == expected_port, f"Default node {port_name} mismatch: expected {expected_port}, got {actual_port}"
    
    # Verify offset node ports
    for port_name, expected_port in expected_offset_ports.items():
        actual_port = node_offset.query(f"SELECT getServerPort('{port_name}')").strip()
        assert actual_port == expected_port, f"Offset node {port_name} mismatch: expected {expected_port}, got {actual_port}"