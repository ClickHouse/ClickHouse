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


def test_simple_query_after_index_reload(start_cluster):
    node.query(
        """
        create table t(a UInt32, b UInt32) engine=MergeTree order by (a, b) settings index_granularity=1;

        -- for this part the first columns is useless, so we have to use both
        insert into t select 42, number from numbers_mt(100);

        -- for this part the first columns is enough
        insert into t select number, number from numbers_mt(100);
        """
    )

    # force reloading index
    node.restart_clickhouse()

    # the bug happened when we used (a, b) index values for one part and only (a) for another in PartsSplitter. even a simple count query is enough,
    # because some granules were assinged to wrong layers and hence not returned from the reading step (because they were filtered out by `FilterSortedStreamByRange`)
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
