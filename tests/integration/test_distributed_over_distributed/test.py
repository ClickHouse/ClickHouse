# This test is a subset of the 01223_dist_on_dist.
# (just in case, with real separate instances).


import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

NODES = {'node' + str(i): cluster.add_instance(
    'node' + str(i),
    main_configs=['configs/remote_servers.xml'],
    user_configs=['configs/set_distributed_defaults.xml'],
) for i in (1, 2)}

CREATE_TABLES_SQL = '''
CREATE TABLE
    base_table(
        node String,
        key Int32,
        value Int32
    )
ENGINE = Memory;

CREATE TABLE
    distributed_table
AS base_table
ENGINE = Distributed(test_cluster, default, base_table);

CREATE TABLE
    distributed_over_distributed_table
AS distributed_table
ENGINE = Distributed('test_cluster', default, distributed_table);
'''

INSERT_SQL_TEMPLATE = "INSERT INTO base_table VALUES ('{node_id}', {key}, {value})"


@pytest.fixture(scope="session")
def started_cluster():
    try:
        cluster.start()
        for node_index, (node_name, node) in enumerate(NODES.items()):
            node.query(CREATE_TABLES_SQL)
            for i in range(0, 2):
                node.query(INSERT_SQL_TEMPLATE.format(node_id=node_name, key=i, value=i + (node_index * 10)))
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("node,source", [
    pytest.param(NODES["node1"], "distributed_over_distributed_table", id="dod_node1"),
    pytest.param(NODES["node1"], "cluster('test_cluster', default, distributed_table)", id="cluster_node1"),
    pytest.param(NODES["node2"], "distributed_over_distributed_table", id="dod_node2"),
    pytest.param(NODES["node2"], "cluster('test_cluster', default, distributed_table)", id="cluster_node2"),
]
)
class TestDistributedOverDistributedSuite:
    def test_select_with_order_by_node(self, started_cluster, node, source):
        assert node.query("SELECT * FROM {source} ORDER BY node, key".format(source=source)) \
               == """node1	0	0
node1	0	0
node1	1	1
node1	1	1
node2	0	10
node2	0	10
node2	1	11
node2	1	11
"""

    def test_select_with_order_by_key(self, started_cluster, node, source):
        assert node.query("SELECT * FROM {source} ORDER BY key, node".format(source=source)) \
               == """node1	0	0
node1	0	0
node2	0	10
node2	0	10
node1	1	1
node1	1	1
node2	1	11
node2	1	11
"""

    def test_select_with_group_by_node(self, started_cluster, node, source):
        assert node.query("SELECT node, SUM(value) FROM {source} GROUP BY node ORDER BY node".format(source=source)) \
               == "node1	2\nnode2	42\n"

    def test_select_with_group_by_key(self, started_cluster, node, source):
        assert node.query("SELECT key, SUM(value) FROM {source} GROUP BY key ORDER BY key".format(source=source)) \
               == "0	20\n1	24\n"

    def test_select_sum(self, started_cluster, node, source):
        assert node.query("SELECT SUM(value) FROM {source}".format(source=source)) \
               == "44\n"
