import pytest
import logging

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_simple_query_after_restart(start_cluster):
    node.query(
        """
        create table t(a UInt32, b UInt32) engine=MergeTree order by (a, b) settings index_granularity=1;

        insert into t select 42, number from numbers_mt(100);
        insert into t select number, number from numbers_mt(100);
        """
    )

    node.restart_clickhouse()

    assert (
        int(
            node.query(
                "select count() from t where not ignore(*)",
                settings={
                    "max_threads": 4,
                    "merge_tree_min_bytes_for_concurrent_read": 1,
                    "merge_tree_min_rows_for_concurrent_read": 1,
                    "merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability": 1,
                },
            )
        )
        == 200
    )
