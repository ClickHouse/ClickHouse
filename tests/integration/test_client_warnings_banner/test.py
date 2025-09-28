import pytest

import helpers.cluster.ClickHouseCluster
import helpers.client.Client

cluster = helpers.cluster.ClickHouseCluster(__file__)

node = cluster.add_instance('node')

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_default(started_cluster):
    try:
        output = node.exec_in_container(
            ['sh', '-c', '/usr/bin/clickhouse client -q "SELECT 1"'],
            privileged = True,
            user = 'root',
        )
    except AssertionError:
        raise
    except Exception as ex:
        print(ex)
    assert 'Warnings:' in output

def test_cli_no_warnings(started_cluster):
    try:
        output = node.exec_in_container(
            ['sh', '-c', '/usr/bin/clickhouse client --no-warnings -q "SELECT 1"'],
            privileged = True,
            user = 'root',
        )
    except AssertionError:
        raise
    except Exception as ex:
        print(ex)
    assert 'Warnings:' not in output

# assert False, 'message'
