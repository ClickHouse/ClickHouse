import logging
import time

import pytest

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
        except Exception:
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


def test_memory_usage_doesnt_include_page_cache_size(started_cluster):
    try:
        # populate page cache with 4GB of data; it might be killed by OOM killer but it is fine
        node1.exec_in_container(
            ["dd", "if=/dev/zero", "of=outputfile", "bs=1M", "count=4K"]
        )
    except Exception:
        pass

    observer_refresh_period = int(
        node1.query(
            "select value from system.server_settings where name = 'cgroups_memory_usage_observer_wait_time'"
        ).strip()
    )
    time.sleep(observer_refresh_period + 1)

    max_mem_usage_from_cgroup = node1.query(
        """
      SELECT max(toUInt64(replaceRegexpAll(message, 'Read current memory usage (\\d+) bytes.*', '\\1'))) AS max_mem
        FROM system.text_log
       WHERE logger_name = 'CgroupsMemoryUsageObserver' AND message LIKE 'Read current memory usage%bytes%'
        """
    ).strip()
    assert int(max_mem_usage_from_cgroup) < 2 * 2**30
