import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1',
    main_configs=['configs/remote_servers.xml'],
    user_configs=['configs/set_distributed_defaults.xml'],
)

node2 = cluster.add_instance('node2',
    main_configs=['configs/remote_servers.xml'],
    user_configs=['configs/set_distributed_defaults.xml'],
)

node3 = cluster.add_instance('node3',
    main_configs=['configs/remote_servers.xml'],
    user_configs=['configs/set_distributed_defaults.xml'],
)


CREATE_TABLES_SQL = '''
CREATE TABLE
    base_table(
        number UInt64,
        string String
    )
ENGINE = Memory;

CREATE TABLE
    distributed_table
AS base_table
ENGINE = Distributed(test_cluster, default, base_table);
'''

INSERT_SQL_TEMPLATE = "INSERT INTO base_table VALUES (42, 'ClickHouse')"

QUERY_WITH_TOTALS = '''
SELECT `number` AS `number`
FROM 
(
    SELECT number AS `number`
    FROM default.distributed_table AS `default.distributed_table`
    WHERE (number >= 42) AND (number <= 42) AND (string = 'ClickHouse')
    GROUP BY `number`
        WITH TOTALS
    HAVING 1
)
WHERE `number` IN 
(
    SELECT number AS `number`
    FROM default.distributed_table AS `default.distributed_table`
    WHERE (number >= 42) AND (number <= 42) AND (string = 'ClickHouse')
)
GROUP BY `number`
'''

QUERY_WITHOUT_TOTALS = '''
SELECT `number` AS `number`
FROM 
(
    SELECT number AS `number`
    FROM default.distributed_table AS `default.distributed_table`
    WHERE (number >= 42) AND (number <= 42) AND (string = 'ClickHouse')
    GROUP BY `number`
)
WHERE `number` IN 
(
    SELECT number AS `number`
    FROM default.distributed_table AS `default.distributed_table`
    WHERE (number >= 42) AND (number <= 42) AND (string = 'ClickHouse')
)
GROUP BY `number`
'''


@pytest.fixture(scope="session")
def started_cluster():
    try:
        cluster.start()
        for node in [node1, node2, node3]:
            node.query(CREATE_TABLES_SQL)
        
        node2.query(INSERT_SQL_TEMPLATE)
        yield cluster

    finally:
        cluster.shutdown()


def test(started_cluster):
    print("with totals", node1.query(QUERY_WITH_TOTALS))
    print("without totals", node1.query(QUERY_WITHOUT_TOTALS))
