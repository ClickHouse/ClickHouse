#!/usr/bin/env python3
import logging
import time
from multiprocessing.dummy import Pool

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/async_metrics_no.xml",
    ],
    mem_limit="4g",
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_multiple_queries():
    if node.is_built_with_sanitizer():
        return

    p = Pool(15)

    def run_query(node):
        try:
            node.query("SELECT * FROM system.numbers GROUP BY number")
        except Exception as ex:
            print("Exception", ex)
            raise ex

    tasks = []
    for i in range(30):
        tasks.append(p.apply_async(run_query, (node,)))
        time.sleep(i * 0.1)

    for task in tasks:
        try:
            task.get()
        except Exception as ex:
            print("Exception", ex)

    # test that we didn't kill the server
    node.query("SELECT 1")
