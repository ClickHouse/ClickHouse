# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest
import time

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/testkeeper.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_projection_broken_part():
    node.query(
        """
        create table test_projection_broken_parts_1 (a int, b int, projection ab (select a, sum(b) group by a))
        engine = ReplicatedMergeTree('/clickhouse-tables/test_projection_broken_parts', 'r1')
        order by a settings index_granularity = 1;

        create table test_projection_broken_parts_2 (a int, b int, projection ab (select a, sum(b) group by a))
        engine ReplicatedMergeTree('/clickhouse-tables/test_projection_broken_parts', 'r2')
        order by a settings index_granularity = 1;

        insert into test_projection_broken_parts_1 values (1, 1), (1, 2), (1, 3);

        system sync replica test_projection_broken_parts_2;
    """
    )

    # break projection part
    node.exec_in_container(
        [
            "bash",
            "-c",
            "rm /var/lib/clickhouse/data/default/test_projection_broken_parts_1/all_0_0_0/ab.proj/data.bin",
        ]
    )

    expected_error = "No such file or directory"
    assert expected_error in node.query_and_get_error(
        "select sum(b) from test_projection_broken_parts_1 group by a"
    )

    time.sleep(2)

    assert (
        int(node.query("select sum(b) from test_projection_broken_parts_1 group by a"))
        == 6
    )
