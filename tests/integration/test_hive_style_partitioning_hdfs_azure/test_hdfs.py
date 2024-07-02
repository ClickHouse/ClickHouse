#!/usr/bin/env python3

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, is_arm
import re

from helpers.cluster import ClickHouseCluster

if is_arm():
    pytestmark = pytest.mark.skip

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/macro_hdfs.xml",
        "configs/schema_cache_hdfs.xml",
        "configs/cluster_hdfs.xml",
    ],
    with_hdfs=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_hdfs_partitioning_with_one_parameter(started_cluster):
    hdfs_api = started_cluster.hdfs_api
    hdfs_api.write_data(
        f"/column0=Elizabeth/parquet_1", f"Elizabeth\tGordon\n"
    )
    assert (
        hdfs_api.read_data(f"/column0=Elizabeth/parquet_1")
        == f"Elizabeth\tGordon\n"
    )

    r = node1.query(
        "SELECT _column0 FROM hdfs('hdfs://hdfs1:9000/column0=Elizabeth/parquet_1', 'TSV')", settings={"hdfs_hive_partitioning": 1}
    )
    assert (r == f"Elizabeth\n")

def test_hdfs_partitioning_with_two_parameters(started_cluster):
    hdfs_api = started_cluster.hdfs_api
    hdfs_api.write_data(
        f"/column0=Elizabeth/column1=Gordon/parquet_2", f"Elizabeth\tGordon\n"
    )
    assert (
        hdfs_api.read_data(f"/column0=Elizabeth/column1=Gordon/parquet_2")
        == f"Elizabeth\tGordon\n"
    )

    r = node1.query(
        "SELECT _column1 FROM hdfs('hdfs://hdfs1:9000/column0=Elizabeth/column1=Gordon/parquet_2', 'TSV');", settings={"hdfs_hive_partitioning": 1}
    )
    assert (r == f"Gordon\n")

def test_hdfs_partitioning_without_setting(started_cluster):
    hdfs_api = started_cluster.hdfs_api
    hdfs_api.write_data(
        f"/column0=Elizabeth/column1=Gordon/parquet_2", f"Elizabeth\tGordon\n"
    )
    assert (
        hdfs_api.read_data(f"/column0=Elizabeth/column1=Gordon/parquet_2")
        == f"Elizabeth\tGordon\n"
    )
    pattern = re.compile(r"DB::Exception: Unknown expression identifier '.*' in scope.*", re.DOTALL)

    with pytest.raises(QueryRuntimeException, match=pattern):
        node1.query(f"SELECT _column1 FROM hdfs('hdfs://hdfs1:9000/column0=Elizabeth/column1=Gordon/parquet_2', 'TSV');", settings={"hdfs_hive_partitioning": 0})

if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
