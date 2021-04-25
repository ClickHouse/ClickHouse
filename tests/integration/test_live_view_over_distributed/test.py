

import sys

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.uclient import client, prompt, end_of_block

cluster = ClickHouseCluster(__file__)
# log = sys.stdout
log = None

NODES = {'node' + str(i): cluster.add_instance(
    'node' + str(i),
    main_configs=['configs/remote_servers.xml'],
    user_configs=['configs/set_distributed_defaults.xml'],
) for i in (1, 2)}

CREATE_TABLES_SQL = '''
DROP TABLE IF EXISTS lv_over_distributed_table;
DROP TABLE IF EXISTS distributed_table;
DROP TABLE IF EXISTS base_table;

SET allow_experimental_live_view = 1;

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
ENGINE = Distributed(test_cluster, default, base_table, rand());

CREATE LIVE VIEW lv_over_distributed_table AS SELECT * FROM distributed_table;
'''

INSERT_SQL_TEMPLATE = "INSERT INTO base_table VALUES ('{node_id}', {key}, {value})"


@pytest.fixture(scope="function")
def started_cluster():
    try:
        cluster.start()
        for node_index, (node_name, node) in enumerate(NODES.items()):
            node.query(CREATE_TABLES_SQL)
            for i in range(0, 2):
                sql = INSERT_SQL_TEMPLATE.format(node_id=node_name, key=i, value=i + (node_index * 10))
                node.query(sql)
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("node", list(NODES.values())[:1])
@pytest.mark.parametrize("source", ["lv_over_distributed_table"])
class TestLiveViewOverDistributedSuite:
    def test_select_with_order_by_node(self, started_cluster, node, source):
        r = node.query("SELECT * FROM {source} ORDER BY node, key".format(source=source))
        assert r == """node1\t0\t0
node1\t1\t1
node2\t0\t10
node2\t1\t11
"""

    def test_select_with_order_by_key(self, started_cluster, node, source):
        assert node.query("SELECT * FROM {source} ORDER BY key, node".format(source=source)) \
               == """node1\t0\t0
node2\t0\t10
node1\t1\t1
node2\t1\t11
"""

    def test_select_with_group_by_node(self, started_cluster, node, source):
        assert node.query("SELECT node, SUM(value) FROM {source} GROUP BY node ORDER BY node".format(source=source)) \
               == "node1\t1\nnode2\t21\n"

    def test_select_with_group_by_key(self, started_cluster, node, source):
        assert node.query("SELECT key, SUM(value) FROM {source} GROUP BY key ORDER BY key".format(source=source)) \
               == "0\t10\n1\t12\n"

    def test_select_sum(self, started_cluster, node, source):
        assert node.query("SELECT SUM(value) FROM {source}".format(source=source)) \
               == "22\n"

    def test_watch_live_view_order_by_node(self, started_cluster, node, source):
        command = " ".join(node.client.command)
        args = dict(log=log, command=command)

        with client(name="client1> ", **args) as client1, client(name="client2> ", **args) as client2:
            client1.expect(prompt)
            client2.expect(prompt)

            client1.send("SET allow_experimental_live_view = 1")
            client1.expect(prompt)
            client2.send("SET allow_experimental_live_view = 1")
            client2.expect(prompt)

            client1.send("DROP TABLE IF EXISTS lv")
            client1.expect(prompt)
            client1.send("CREATE LIVE VIEW lv AS SELECT * FROM distributed_table ORDER BY node, key")
            client1.expect(prompt)

            client1.send("WATCH lv FORMAT CSV")
            client1.expect('"node1",0,0,1')
            client1.expect('"node1",1,1,1')
            client1.expect('"node2",0,10,1')
            client1.expect('"node2",1,11,1')

            client2.send("INSERT INTO distributed_table VALUES ('node1', 2, 2)")
            client2.expect(prompt)
            client1.expect('"node1",0,0,2')
            client1.expect('"node1",1,1,2')
            client1.expect('"node1",2,2,2')
            client1.expect('"node2",0,10,2')
            client1.expect('"node2",1,11,2')

            client2.send("INSERT INTO distributed_table VALUES ('node1', 0, 3), ('node3', 3, 3)")
            client2.expect(prompt)
            client1.expect('"node1",0,0,3')
            client1.expect('"node1",0,3,3')
            client1.expect('"node1",1,1,3')
            client1.expect('"node1",2,2,3')
            client1.expect('"node2",0,10,3')
            client1.expect('"node2",1,11,3')
            client1.expect('"node3",3,3,3')

    def test_watch_live_view_order_by_key(self, started_cluster, node, source):
        command = " ".join(node.client.command)
        args = dict(log=log, command=command)

        with client(name="client1> ", **args) as client1, client(name="client2> ", **args) as client2:
            client1.expect(prompt)
            client2.expect(prompt)

            client1.send("SET allow_experimental_live_view = 1")
            client1.expect(prompt)
            client2.send("SET allow_experimental_live_view = 1")
            client2.expect(prompt)

            client1.send("DROP TABLE IF EXISTS lv")
            client1.expect(prompt)
            client1.send("CREATE LIVE VIEW lv AS SELECT * FROM distributed_table ORDER BY key, node")
            client1.expect(prompt)

            client1.send("WATCH lv FORMAT CSV")
            client1.expect('"node1",0,0,1')
            client1.expect('"node2",0,10,1')
            client1.expect('"node1",1,1,1')
            client1.expect('"node2",1,11,1')

            client2.send("INSERT INTO distributed_table VALUES ('node1', 2, 2)")
            client2.expect(prompt)
            client1.expect('"node1",0,0,2')
            client1.expect('"node2",0,10,2')
            client1.expect('"node1",1,1,2')
            client1.expect('"node2",1,11,2')
            client1.expect('"node1",2,2,2')

            client2.send("INSERT INTO distributed_table VALUES ('node1', 0, 3), ('node3', 3, 3)")
            client2.expect(prompt)
            client1.expect('"node1",0,0,3')
            client1.expect('"node1",0,3,3')
            client1.expect('"node2",0,10,3')
            client1.expect('"node1",1,1,3')
            client1.expect('"node2",1,11,3')
            client1.expect('"node1",2,2,3')
            client1.expect('"node3",3,3,3')

    def test_watch_live_view_group_by_node(self, started_cluster, node, source):
        command = " ".join(node.client.command)
        args = dict(log=log, command=command)

        with client(name="client1> ", **args) as client1, client(name="client2> ", **args) as client2:
            client1.expect(prompt)
            client2.expect(prompt)

            client1.send("SET allow_experimental_live_view = 1")
            client1.expect(prompt)
            client2.send("SET allow_experimental_live_view = 1")
            client2.expect(prompt)

            client1.send("DROP TABLE IF EXISTS lv")
            client1.expect(prompt)
            client1.send(
                "CREATE LIVE VIEW lv AS SELECT node, SUM(value) FROM distributed_table GROUP BY node ORDER BY node")
            client1.expect(prompt)

            client1.send("WATCH lv FORMAT CSV")
            client1.expect('"node1",1,1')
            client1.expect('"node2",21,1')

            client2.send("INSERT INTO distributed_table VALUES ('node1', 2, 2)")
            client2.expect(prompt)
            client1.expect('"node1",3,2')
            client1.expect('"node2",21,2')

            client2.send("INSERT INTO distributed_table VALUES ('node1', 0, 3), ('node3', 3, 3)")
            client2.expect(prompt)
            client1.expect('"node1",6,3')
            client1.expect('"node2",21,3')
            client1.expect('"node3",3,3')

    def test_watch_live_view_group_by_key(self, started_cluster, node, source):
        command = " ".join(node.client.command)
        args = dict(log=log, command=command)
        sep = ' \xe2\x94\x82'
        with client(name="client1> ", **args) as client1, client(name="client2> ", **args) as client2:
            client1.expect(prompt)
            client2.expect(prompt)

            client1.send("SET allow_experimental_live_view = 1")
            client1.expect(prompt)
            client2.send("SET allow_experimental_live_view = 1")
            client2.expect(prompt)

            client1.send("DROP TABLE IF EXISTS lv")
            client1.expect(prompt)
            client1.send(
                "CREATE LIVE VIEW lv AS SELECT key, SUM(value) FROM distributed_table GROUP BY key ORDER BY key")
            client1.expect(prompt)

            client1.send("WATCH lv FORMAT CSV")
            client1.expect('0,10,1')
            client1.expect('1,12,1')

            client2.send("INSERT INTO distributed_table VALUES ('node1', 2, 2)")
            client2.expect(prompt)
            client1.expect('0,10,2')
            client1.expect('1,12,2')
            client1.expect('2,2,2')

            client2.send("INSERT INTO distributed_table VALUES ('node1', 0, 3), ('node1', 3, 3)")
            client2.expect(prompt)
            client1.expect('0,13,3')
            client1.expect('1,12,3')
            client1.expect('2,2,3')
            client1.expect('3,3,3')

    def test_watch_live_view_sum(self, started_cluster, node, source):
        command = " ".join(node.client.command)
        args = dict(log=log, command=command)

        with client(name="client1> ", **args) as client1, client(name="client2> ", **args) as client2:
            client1.expect(prompt)
            client2.expect(prompt)

            client1.send("SET allow_experimental_live_view = 1")
            client1.expect(prompt)
            client2.send("SET allow_experimental_live_view = 1")
            client2.expect(prompt)

            client1.send("DROP TABLE IF EXISTS lv")
            client1.expect(prompt)
            client1.send("CREATE LIVE VIEW lv AS SELECT sum(value) FROM distributed_table")
            client1.expect(prompt)

            client1.send("WATCH lv")
            client1.expect(r"22.*1" + end_of_block)

            client2.send("INSERT INTO distributed_table VALUES ('node1', 2, 2)")
            client2.expect(prompt)
            client1.expect(r"24.*2" + end_of_block)

            client2.send("INSERT INTO distributed_table VALUES ('node1', 3, 3), ('node1', 4, 4)")
            client2.expect(prompt)
            client1.expect(r"31.*3" + end_of_block)
