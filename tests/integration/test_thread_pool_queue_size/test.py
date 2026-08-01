#!/usr/bin/env python3

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/83273
# Setting a thread pool *_queue_size server setting to a very large value (e.g. the max
# uint64) used to throw std::length_error from jobs.reserve() while reconfiguring the
# static thread pools, both on startup and on SYSTEM RELOAD CONFIG. The server must
# start and keep working: queue_size is only a logical limit, the pre-reservation is
# bounded.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/huge_queue_size.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_huge_queue_size_does_not_crash(start_cluster):
    # The server starts despite the huge queue sizes in the config.
    assert node.query("SELECT 1") == "1\n"

    assert (
        node.query(
            "SELECT value FROM system.server_settings WHERE name = 'io_thread_pool_queue_size'"
        )
        == "18446744073709551615\n"
    )

    # Reconfiguration on reload must not throw either (this is the path from the issue's
    # stack trace: StaticThreadPool::reloadConfiguration -> ThreadPool::setQueueSize).
    node.query("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT 1") == "1\n"
