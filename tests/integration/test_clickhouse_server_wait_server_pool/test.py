import pytest
import threading
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["configs/overrides.yaml"], stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def test(start_cluster):
    node.restart_clickhouse()

    def run_query():
        node.query("select sleepEachRow(1) from numbers(10)", settings={"function_sleep_max_microseconds_per_block": 0})

    t = threading.Thread(target=run_query)
    t.start()

    for i in range(100):
        node.query("select count() from system.processes where query_id = '$query_id'")
        time.sleep(0.1)
    assert node.stop_clickhouse()
    t.join()
