import pytest
import uuid

from helpers.cluster import ClickHouseCluster
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
    result = node.query(f"SELECT * FROM arrowflight('arrowflight1:5005', 'ABC')")
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
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
        f"SELECT * FROM arrowflight('arrowflight1:5005', '{dataset}') ORDER BY column1"
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
