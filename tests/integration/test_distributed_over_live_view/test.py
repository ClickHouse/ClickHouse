

import sys
import time

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
DROP TABLE IF EXISTS lv_over_base_table;
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

CREATE LIVE VIEW lv_over_base_table AS SELECT * FROM base_table;

CREATE TABLE
    distributed_table
AS base_table
ENGINE = Distributed(test_cluster, default, base_table, rand());
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

def poll_query(node, query, expected, timeout):
    """Repeatedly execute query until either expected result is returned or timeout occurs.
    """
    start_time = time.time()
    while node.query(query) != expected and time.time() - start_time < timeout:
        pass
    assert node.query(query) == expected

@pytest.mark.parametrize("node", list(NODES.values())[:1])
@pytest.mark.parametrize("source", ["lv_over_distributed_table"])
class TestLiveViewOverDistributedSuite:
    def test_distributed_over_live_view_order_by_node(self, started_cluster, node, source):
        node0, node1 = list(NODES.values())

        select_query = "SELECT * FROM distributed_over_lv ORDER BY node, key FORMAT CSV"
        select_query_dist_table = "SELECT * FROM distributed_table ORDER BY node, key FORMAT CSV"
        select_count_query = "SELECT count() FROM distributed_over_lv"

        with client(name="client1> ", log=log, command=" ".join(node0.client.command)) as client1, \
                client(name="client2> ", log=log, command=" ".join(node1.client.command)) as client2:
            client1.expect(prompt)
            client2.expect(prompt)

            client1.send("DROP TABLE IF EXISTS distributed_over_lv")
            client1.expect(prompt)
            client1.send(
                "CREATE TABLE distributed_over_lv AS lv_over_base_table ENGINE = Distributed(test_cluster, default, lv_over_base_table)")
            client1.expect(prompt)

            client1.send(select_query)
            client1.expect('"node1",0,0')
            client1.expect('"node1",1,1')
            client1.expect('"node2",0,10')
            client1.expect('"node2",1,11')
            client1.expect(prompt)

            client1.send("INSERT INTO distributed_table VALUES ('node1', 1, 3), ('node1', 2, 3)")
            client1.expect(prompt)
            client2.send("INSERT INTO distributed_table VALUES ('node1', 3, 3)")
            client2.expect(prompt)

            poll_query(node0, select_count_query, "7\n", timeout=60)
            print("\n--DEBUG1--")
            print(select_query)
            print(node0.query(select_query))
            print("---------")
            print("\n--DEBUG2--")
            print(select_query_dist_table)
            print(node0.query(select_query_dist_table))
            print("---------")

            client1.send(select_query)
            client1.expect('"node1",0,0')
            client1.expect('"node1",1,1')
            client1.expect('"node1",1,3')
            client1.expect('"node1",2,3')
            client1.expect('"node1",3,3')
            client1.expect('"node2",0,10')
            client1.expect('"node2",1,11')
            client1.expect(prompt)

    def test_distributed_over_live_view_order_by_key(self, started_cluster, node, source):
        node0, node1 = list(NODES.values())

        select_query = "SELECT * FROM distributed_over_lv ORDER BY key, node FORMAT CSV"
        select_count_query = "SELECT count() FROM distributed_over_lv"

        with client(name="client1> ", log=log, command=" ".join(node0.client.command)) as client1, \
                client(name="client2> ", log=log, command=" ".join(node1.client.command)) as client2:
            client1.expect(prompt)
            client2.expect(prompt)

            client1.send("DROP TABLE IF EXISTS distributed_over_lv")
            client1.expect(prompt)
            client1.send(
                "CREATE TABLE distributed_over_lv AS lv_over_base_table ENGINE = Distributed(test_cluster, default, lv_over_base_table)")
            client1.expect(prompt)

            client1.send(select_query)
            client1.expect('"node1",0,0')
            client1.expect('"node2",0,10')
            client1.expect('"node1",1,1')
            client1.expect('"node2",1,11')
            client1.expect(prompt)

            client1.send("INSERT INTO distributed_table VALUES ('node1', 1, 3), ('node1', 2, 3)")
            client1.expect(prompt)
            client2.send("INSERT INTO distributed_table VALUES ('node1', 3, 3)")
            client2.expect(prompt)

            poll_query(node0, select_count_query, "7\n", timeout=60)

            client1.send(select_query)
            client1.expect('"node1",0,0')
            client1.expect('"node2",0,10')
            client1.expect('"node1",1,1')
            client1.expect('"node1",1,3')
            client1.expect('"node2",1,11')
            client1.expect('"node1",2,3')
            client1.expect('"node1",3,3')
            client1.expect(prompt)

    def test_distributed_over_live_view_group_by_node(self, started_cluster, node, source):
        node0, node1 = list(NODES.values())

        select_query = "SELECT node, SUM(value) FROM distributed_over_lv GROUP BY node ORDER BY node FORMAT CSV"

        with client(name="client1> ", log=log, command=" ".join(node0.client.command)) as client1, \
                client(name="client2> ", log=log, command=" ".join(node1.client.command)) as client2:
            client1.expect(prompt)
            client2.expect(prompt)

            client1.send("DROP TABLE IF EXISTS distributed_over_lv")
            client1.expect(prompt)
            client1.send(
                "CREATE TABLE distributed_over_lv AS lv_over_base_table ENGINE = Distributed(test_cluster, default, lv_over_base_table)")
            client1.expect(prompt)

            client1.send(select_query)
            client1.expect('"node1",1')
            client1.expect('"node2",21')
            client1.expect(prompt)

            client2.send("INSERT INTO distributed_table VALUES ('node1', 2, 2)")
            client2.expect(prompt)

            poll_query(node0, select_query, '"node1",3\n"node2",21\n', timeout=60)

            client1.send(select_query)
            client1.expect('"node1",3')
            client1.expect('"node2",21')
            client1.expect(prompt)

            client1.send("INSERT INTO distributed_table VALUES ('node1', 1, 3), ('node1', 3, 3)")
            client1.expect(prompt)
            client2.send("INSERT INTO distributed_table VALUES ('node1', 3, 3)")
            client2.expect(prompt)

            poll_query(node0, select_query, '"node1",12\n"node2",21\n', timeout=60)

            client1.send(select_query)
            client1.expect('"node1",12')
            client1.expect('"node2",21')
            client1.expect(prompt)

    def test_distributed_over_live_view_group_by_key(self, started_cluster, node, source):
        node0, node1 = list(NODES.values())

        select_query = "SELECT key, SUM(value) FROM distributed_over_lv GROUP BY key ORDER BY key FORMAT CSV"

        with client(name="client1> ", log=log, command=" ".join(node0.client.command)) as client1, \
                client(name="client2> ", log=log, command=" ".join(node1.client.command)) as client2:
            client1.expect(prompt)
            client2.expect(prompt)

            client1.send("DROP TABLE IF EXISTS distributed_over_lv")
            client1.expect(prompt)
            client1.send(
                "CREATE TABLE distributed_over_lv AS lv_over_base_table ENGINE = Distributed(test_cluster, default, lv_over_base_table)")
            client1.expect(prompt)

            client1.send(select_query)
            client1.expect('0,10')
            client1.expect('1,12')
            client1.expect(prompt)

            client2.send("INSERT INTO distributed_table VALUES ('node1', 2, 2)")
            client2.expect(prompt)

            poll_query(node0, "SELECT count() FROM (%s)" % select_query.rsplit("FORMAT")[0], "3\n", timeout=60)

            client1.send(select_query)
            client1.expect('0,10')
            client1.expect('1,12')
            client1.expect('2,2')
            client1.expect(prompt)

            client2.send("INSERT INTO distributed_table VALUES ('node1', 1, 3), ('node1', 3, 3)")
            client2.expect(prompt)

            poll_query(node0, "SELECT count() FROM (%s)" % select_query.rsplit("FORMAT")[0], "4\n", timeout=60)

            client1.send(select_query)
            client1.expect('0,10')
            client1.expect('1,15')
            client1.expect('2,2')
            client1.expect('3,3')
            client1.expect(prompt)

    def test_distributed_over_live_view_sum(self, started_cluster, node, source):
        node0, node1 = list(NODES.values())

        with client(name="client1> ", log=log, command=" ".join(node0.client.command)) as client1, \
                client(name="client2> ", log=log, command=" ".join(node1.client.command)) as client2:
            client1.expect(prompt)
            client2.expect(prompt)

            client1.send("DROP TABLE IF EXISTS distributed_over_lv")
            client1.expect(prompt)
            client1.send(
                "CREATE TABLE distributed_over_lv AS lv_over_base_table ENGINE = Distributed(test_cluster, default, lv_over_base_table)")
            client1.expect(prompt)

            client1.send("SELECT sum(value) FROM distributed_over_lv")
            client1.expect(r"22" + end_of_block)
            client1.expect(prompt)

            client2.send("INSERT INTO distributed_table VALUES ('node1', 2, 2)")
            client2.expect(prompt)

            poll_query(node0, "SELECT sum(value) FROM distributed_over_lv", "24\n", timeout=60)

            client2.send("INSERT INTO distributed_table VALUES ('node1', 3, 3), ('node1', 4, 4)")
            client2.expect(prompt)

            poll_query(node0, "SELECT sum(value) FROM distributed_over_lv", "31\n", timeout=60)
