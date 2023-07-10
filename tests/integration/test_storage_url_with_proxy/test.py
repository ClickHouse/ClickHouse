import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/conf.xml",
        "configs/named_collections.xml",
        "configs/proxy_list.xml"
    ],
    user_configs=["configs/users.xml"],
    with_nginx=True,
    with_minio=True,
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
