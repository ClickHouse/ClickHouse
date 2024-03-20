import pytest
import logging
import time

from helpers.cluster import ClickHouseCluster, run_and_check

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["config/text_log.xml"], mem_limit="5g"
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_latest_mem_limit():
    for _ in range(10):
        try:
            mem_limit = float(
                node1.query(
                    """
                        select extract(message, '\\d+\\.\\d+') from system.text_log
                        where message like '%Setting max_server_memory_usage was set to%' and
                        message not like '%like%' order by event_time desc limit 1
                    """
                ).strip()
            )
            return mem_limit
        except Exception as e:
            time.sleep(1)
    raise Exception("Cannot get memory limit")


def test_observe_memory_limit(started_cluster):
    original_max_mem = get_latest_mem_limit()
    logging.debug(f"get original memory limit {original_max_mem}")
    run_and_check(["docker", "update", "--memory=10g", node1.docker_id])
    for _ in range(30):
        time.sleep(10)
        new_max_mem = get_latest_mem_limit()
        logging.debug(f"get new memory limit {new_max_mem}")
        if new_max_mem > original_max_mem:
            return
    raise Exception("the memory limit does not increase as expected")
