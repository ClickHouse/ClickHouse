import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["configs/overrides.yaml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def test(start_cluster):
    try:
        node.query(
            f"""
            DROP TABLE IF EXISTS t_proj_external;
            CREATE TABLE t_proj_external (k1 UInt32, k2 UInt32, k3 UInt32, value UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
            INSERT INTO t_proj_external SELECT 1, number%2, number%4, number FROM numbers(50000);
            """
        )

        with pytest.raises(
            QueryRuntimeException,
            match="Limit for temporary files size exceeded.* 10240 bytes",
        ):
            node.query("SELECT sumMap([number], [number]) FROM numbers(1e6) group by number%100000 format Null settings max_threads=1, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0, group_by_two_level_threshold = 1, max_block_size=1000")

        for _ in range(10):
            node.query("SELECT k1, k2, k3, sum(value) v FROM t_proj_external GROUP BY k1, k2, k3 ORDER BY k1, k2, k3 SETTINGS optimize_aggregation_in_order = 0, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0, group_by_two_level_threshold = 1, max_block_size=65000")

    finally:
        node.query("DROP TABLE IF EXISTS t_proj_external SYNC")
