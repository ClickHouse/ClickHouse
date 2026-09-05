import pytest
import uuid

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import arrowflight_user, arrowflight_pass
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_arrowflight=True, stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_table_function():
    result = node.query(f"SELECT * FROM arrowFlight('arrowflight1:5005', 'ABC')")
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
    )
    
    # test that dataset_name is being sent correctly to the arrowflight server
    result = node.query(f"SELECT * FROM arrowFlight('arrowflight1:5005', 'XYZ')")
    assert result == TSV(
        [
            ["1", "4"],
            ["2", "5"],
            ["3", "6"],
        ]
    )


def test_table_function_old_name():
    # "arrowflight" is an obsolete name.
    result = node.query(f"SELECT * FROM arrowflight('arrowflight1:5005', 'ABC')")
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
    )
    
    result = node.query(f"SELECT * FROM arrowflight('arrowflight1:5005', 'XYZ')")
    assert result == TSV(
        [
            ["1", "4"],
            ["2", "5"],
            ["3", "6"],
        ]
    )


def test_table_function_with_auth():
    result = node.query(
        f"SELECT * FROM arrowFlight('arrowflight1:5006', 'ABC', '{arrowflight_user}', '{arrowflight_pass}')"
    )
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
    )

    assert "No credentials supplied" in node.query_and_get_error(
        f"SELECT * FROM arrowFlight('arrowflight1:5006', 'ABC')"
    )
    assert "Unknown user" in node.query_and_get_error(
        f"SELECT * FROM arrowFlight('arrowflight1:5006', 'ABC', 'default', '')"
    )
    assert "Wrong password" in node.query_and_get_error(
        f"SELECT * FROM arrowFlight('arrowflight1:5006', 'ABC', '{arrowflight_user}', 'qwe123')"
    )


def test_arrowflight_storage():
    dataset = uuid.uuid4().hex

    node.query(
        f"""
        CREATE TABLE arrow_test (
            column1 String,
            column2 String
        ) ENGINE=ArrowFlight('arrowflight1:5005', '{dataset}')
        """
    )

    assert node.query(f"SELECT * FROM arrow_test") == ""

    node.query(
        "INSERT INTO arrow_test VALUES ('a','data_a'), ('b','data_b'), ('c','data_c')"
    )

    result = node.query(f"SELECT * FROM arrow_test ORDER BY column1")
    assert result == TSV(
        [
            ["a", "data_a"],
            ["b", "data_b"],
            ["c", "data_c"],
        ]
    )

    node.query("INSERT INTO arrow_test VALUES ('x','data_x'), ('y','data_y')")

    new_result = node.query(f"SELECT * FROM arrow_test ORDER BY column1")
    assert new_result == TSV(
        [
            ["a", "data_a"],
            ["b", "data_b"],
            ["c", "data_c"],
            ["x", "data_x"],
            ["y", "data_y"],
        ]
    )

    table_func_result = node.query(
        f"SELECT * FROM arrowFlight('arrowflight1:5005', '{dataset}') ORDER BY column1"
    )
    assert table_func_result == TSV(
        [
            ["a", "data_a"],
            ["b", "data_b"],
            ["c", "data_c"],
            ["x", "data_x"],
            ["y", "data_y"],
        ]
    )

    node.query("DROP TABLE arrow_test")


def test_table_function_with_named_collection():
    """Test that ArrowFlight table function works with named collections and dataset parameter."""
    # Create a named collection for ArrowFlight
    node.query("""
        CREATE NAMED COLLECTION arrowflight_test_collection AS
        host = 'arrowflight1',
        port = 5005,
        dataset = 'ABC',
        use_basic_authentication = False
    """)
    
    # Test that the table function works with the named collection
    result = node.query("SELECT * FROM arrowFlight(arrowflight_test_collection)")
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
    )
    
    # Test that different dataset param causes different data to be returned
    result_xyz = node.query("SELECT * FROM arrowFlight(arrowflight_test_collection, dataset = 'XYZ')")
    assert result_xyz == TSV(
        [
            ["1", "4"],
            ["2", "5"],
            ["3", "6"],
        ]
    )
    
    
    
    # Clean up
    node.query("DROP NAMED COLLECTION arrowflight_test_collection")


def test_table_function_with_named_collection_auth():
    """Test that ArrowFlight table function works with named collections including authentication."""
    # Create a named collection with authentication
    node.query(f"""
        CREATE NAMED COLLECTION arrowflight_auth_collection AS
        host = 'arrowflight1',
        port = 5006,
        dataset = 'ABC',
        username = '{arrowflight_user}',
        password = '{arrowflight_pass}',
        use_basic_authentication = True
    """)
    
    # Test that the table function works with the named collection
    result = node.query("SELECT * FROM arrowFlight(arrowflight_auth_collection)")
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
    )
    
    result_xyz = node.query("SELECT * FROM arrowFlight(arrowflight_auth_collection, dataset = 'XYZ')")
    assert result_xyz == TSV(
        [
            ["1", "4"],
            ["2", "5"],
            ["3", "6"],
        ]
    )
    
    # Clean up
    node.query("DROP NAMED COLLECTION arrowflight_auth_collection")


def test_arrowflight_storage_with_named_collection():
    """Test that ArrowFlight storage engine works with named collections and dataset parameter."""
    dataset1 = uuid.uuid4().hex
    dataset2 = uuid.uuid4().hex
    # Create a named collection for ArrowFlight storage
    node.query(f"""
        CREATE NAMED COLLECTION arrowflight_storage_collection AS
        host = 'arrowflight1',
        port = 5005,
        dataset = '{dataset1}',
        use_basic_authentication = False
    """)
    
    # Create table using the named collection
    node.query(f"""
        CREATE TABLE arrow_test_named (
            column1 String,
            column2 String
        ) ENGINE=ArrowFlight(arrowflight_storage_collection, dataset = '{dataset1}')
    """)
    
    node.query(f"""
        CREATE TABLE arrow_test_named_2 (
            column1 String,
            column2 String
        ) ENGINE=ArrowFlight(arrowflight_storage_collection, dataset = '{dataset2}')
    """)
    
    # Insert data
    node.query(
        "INSERT INTO arrow_test_named VALUES ('a','data_a'), ('b','data_b'), ('c','data_c')"
    )
    
    node.query(
        "INSERT INTO arrow_test_named_2 VALUES ('x','data_x'), ('y','data_y')"
    )
    
    # Verify data can be read
    result = node.query("SELECT * FROM arrow_test_named ORDER BY column1")
    assert result == TSV(
        [
            ["a", "data_a"],
            ["b", "data_b"],
            ["c", "data_c"],
        ]
    )
    
    result_xyz = node.query("SELECT * FROM arrow_test_named_2 ORDER BY column1")
    assert result_xyz == TSV(
        [
            ["x", "data_x"],
            ["y", "data_y"],
        ]
    )
    
    # Test table function with the same dataset
    table_func_result = node.query(
        f"SELECT * FROM arrowFlight('arrowflight1:5005', '{dataset1}') ORDER BY column1"
    )
    assert table_func_result == TSV(
        [
            ["a", "data_a"],
            ["b", "data_b"],
            ["c", "data_c"],
        ]
    )
    
    table_func_result_xyz = node.query(
        f"SELECT * FROM arrowFlight('arrowflight1:5005', '{dataset2}') ORDER BY column1"
    )
    assert table_func_result_xyz == TSV(
        [
            ["x", "data_x"],
            ["y", "data_y"],
        ]
    )
    
    # Clean up
    node.query("DROP TABLE arrow_test_named")
    node.query("DROP TABLE arrow_test_named_2")
    node.query("DROP NAMED COLLECTION arrowflight_storage_collection")
