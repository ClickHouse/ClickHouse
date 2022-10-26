import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/conf.xml"], with_nginx=True
)


@pytest.fixture(scope="module", autouse=True)
def setup_node():
    try:
        cluster.start()
        node1.query(
            "insert into table function url(url1) partition by column3 values (1, 2, 3), (3, 2, 1), (1, 3, 2)"
        )
        yield
    finally:
        cluster.shutdown()


def test_partition_by():
    result = node1.query(
        f"select * from url('http://nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "3\t2\t1"
    result = node1.query(
        f"select * from url('http://nginx:80/test_2', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "1\t3\t2"
    result = node1.query(
        f"select * from url('http://nginx:80/test_3', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')"
    )
    assert result.strip() == "1\t2\t3"


def test_table_function_url_access_rights():
    node1.query("CREATE USER OR REPLACE u1")

    expected_error = "necessary to have grant CREATE TEMPORARY TABLE, URL ON *.*"
    assert expected_error in node1.query_and_get_error(
        f"SELECT * FROM url('http://nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')",
        user="u1",
    )

    expected_error = "necessary to have grant CREATE TEMPORARY TABLE, URL ON *.*"
    assert expected_error in node1.query_and_get_error(
        f"SELECT * FROM url('http://nginx:80/test_1', 'TSV')", user="u1"
    )

    assert node1.query(
        f"DESCRIBE TABLE url('http://nginx:80/test_1', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')",
        user="u1",
    ) == TSV([["column1", "UInt32"], ["column2", "UInt32"], ["column3", "UInt32"]])

    assert node1.query(
        f"DESCRIBE TABLE url('http://nginx:80/not-exist', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')",
        user="u1",
    ) == TSV([["column1", "UInt32"], ["column2", "UInt32"], ["column3", "UInt32"]])

    expected_error = "necessary to have grant URL ON *.*"
    assert expected_error in node1.query_and_get_error(
        f"DESCRIBE TABLE url('http://nginx:80/test_1', 'TSV')", user="u1"
    )

    node1.query("GRANT URL ON *.* TO u1")
    assert node1.query(
        f"DESCRIBE TABLE url('http://nginx:80/test_1', 'TSV')",
        user="u1",
    ) == TSV(
        [
            ["c1", "Nullable(Int64)"],
            ["c2", "Nullable(Int64)"],
            ["c3", "Nullable(Int64)"],
        ]
    )
