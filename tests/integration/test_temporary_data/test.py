# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_tmp_data_no_leftovers(start_cluster):
    q = node.get_query_request

    settings = {
        "max_bytes_before_external_group_by": "10K",
        "max_bytes_before_external_sort": "10K",
        "join_algorithm": "grace_hash",
        "max_bytes_in_join": "10K",
        "grace_hash_join_initial_buckets": "16",
    }

    # Run some queries in the background to generate temporary data
    q(
        "SELECT ignore(*) FROM numbers(10 * 1024 * 1024) ORDER BY sipHash64(number)",
        settings=settings,
    )
    q("SELECT * FROM system.numbers GROUP BY ALL", settings=settings)
    q(
        "SELECT * FROM system.numbers as t1 JOIN system.numbers as t2 USING (number)",
        settings=settings,
    )

    # Wait a bit to make sure the temporary data is written to disk
    time.sleep(5)

    # Hard restart the node
    node.restart_clickhouse(kill=True)
    path_to_data = "/var/lib/clickhouse/"

    # Check that there are no temporary files left
    result = node.exec_in_container(["ls", path_to_data + "tmp/"])
    assert result == ""
