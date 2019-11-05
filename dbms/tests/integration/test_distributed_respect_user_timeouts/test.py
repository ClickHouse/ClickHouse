import itertools
import timeit

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV


cluster = ClickHouseCluster(__file__)

NODES = {'node' + str(i): cluster.add_instance(
    'node' + str(i),
    main_configs=['configs/remote_servers.xml'],
    user_configs=['configs/set_distributed_defaults.xml'],
) for i in (1, 2)}

CREATE_TABLES_SQL = '''
CREATE DATABASE test;

CREATE TABLE base_table(
    node String
)
ENGINE = MergeTree
PARTITION BY node
ORDER BY node;

CREATE TABLE distributed_table
ENGINE = Distributed(test_cluster, default, base_table) AS base_table;
'''

INSERT_SQL_TEMPLATE = "INSERT INTO base_table VALUES ('{node_id}')"

SELECTS_SQL = {
    'distributed': 'SELECT node FROM distributed_table ORDER BY node',
    'remote': ("SELECT node FROM remote('node1,node2', default.base_table) "
               "ORDER BY node"),
}

EXCEPTION_NETWORK = 'e.displayText() = DB::NetException: '
EXCEPTION_TIMEOUT = 'Timeout exceeded while reading from socket ('
EXCEPTION_CONNECT = 'Timeout: connect timed out: '

TIMEOUT_MEASUREMENT_EPS = 0.01

EXPECTED_BEHAVIOR = {
    'default': {
        'times': 3,
        'timeout': 1,
    },
    'ready_to_wait': {
        'times': 5,
        'timeout': 3,
    },
}


def _check_exception(exception, expected_tries=3):
    lines = exception.split('\n')

    assert len(lines) > 4, "Unexpected exception (expected: timeout info)"

    assert lines[0].startswith('Received exception from server (version')

    assert lines[1].startswith('Code: 279')
    assert lines[1].endswith('All connection tries failed. Log: ')

    assert lines[2] == '', "Unexpected exception text (expected: empty line)"

    for i, line in enumerate(lines[3:3 + expected_tries]):
        expected_lines = (
            'Code: 209, ' + EXCEPTION_NETWORK + EXCEPTION_TIMEOUT,
            'Code: 209, ' + EXCEPTION_NETWORK + EXCEPTION_CONNECT,
        )

        assert any(line.startswith(expected) for expected in expected_lines), \
            'Unexpected exception at one of the connection attempts'

    assert lines[3 + expected_tries] == '', 'Wrong number of connect attempts'


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node_id, node in NODES.items():
            node.query(CREATE_TABLES_SQL)
            node.query(INSERT_SQL_TEMPLATE.format(node_id=node_id))

        yield cluster

    finally:
        cluster.shutdown()


def _check_timeout_and_exception(node, user, query_base):
    repeats = EXPECTED_BEHAVIOR[user]['times']
    expected_timeout = EXPECTED_BEHAVIOR[user]['timeout'] * repeats

    start = timeit.default_timer()
    exception = node.query_and_get_error(SELECTS_SQL[query_base], user=user)

    # And it should timeout no faster than:
    measured_timeout = timeit.default_timer() - start

    assert measured_timeout >= expected_timeout - TIMEOUT_MEASUREMENT_EPS

    # And exception should reflect connection attempts:
    _check_exception(exception, repeats)


@pytest.mark.parametrize(
    ('first_user', 'node_name', 'query_base'),
    tuple(itertools.product(EXPECTED_BEHAVIOR, NODES, SELECTS_SQL)),
)
def test_reconnect(started_cluster, node_name, first_user, query_base):
    node = NODES[node_name]

    # Everything is up, select should work:
    assert TSV(node.query(SELECTS_SQL[query_base],
                          user=first_user)) == TSV('node1\nnode2')

    with PartitionManager() as pm:
        # Break the connection.
        pm.partition_instances(*NODES.values())

        # Now it shouldn't:
        _check_timeout_and_exception(node, first_user, query_base)

        # Other user should have different timeout and exception
        _check_timeout_and_exception(
            node,
            'default' if first_user != 'default' else 'ready_to_wait',
            query_base,
        )

    # select should work again:
    assert TSV(node.query(SELECTS_SQL[query_base],
                          user=first_user)) == TSV('node1\nnode2')
