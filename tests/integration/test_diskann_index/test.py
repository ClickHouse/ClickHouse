import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def start_cluster():
    print("cluster")
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_condition(start_cluster):
    pass
    # node.query(
    #     f"""
    #     CREATE TABLE diskann_test (
    #         id UInt32, 
    #         url String, 
    #         embedding Tuple({"Float32, " * 511}Float32)
    #     ) Engine=MergeTree ORDER BY id;
    #     """
    # )

    # node.query(
    #     """
    #     INSERT INTO diskann_test FROM INFILE './laion_cut.csv';
    #     """
    # )

    #select_query = "SELECT * from diskann_test WHERE L2Distance(number, tuple(10000.0, 10000.0, 10000.0)) < 30.0"
    #print(node.query(select_query))
    
    #assert node.query(select_query) == "(0,'data'),(1,'data')"
