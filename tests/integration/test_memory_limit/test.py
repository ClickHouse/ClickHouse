#!/usr/bin/env python3
import time
from multiprocessing.dummy import Pool
from pathlib import Path
from uuid import uuid4

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/async_metrics_no.xml",
    ],
    user_configs=[
        "configs/disable_max_bytes_ratio_before_external_group_by.xml",
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
        try:
            node.query("SYSTEM FLUSH LOGS")
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


def test_native_context_setup_error_with_throwing_allocations():
    if node.is_built_with_sanitizer():
        pytest.skip("Requires the generic allocation interceptors")

    # This server setting cannot be changed by a stateless query.
    config = Path(__file__).with_name("configs").joinpath("async_metrics_no.xml").read_text()
    config = config.replace(
        "</clickhouse>",
        "<min_allocation_size_to_throw_on_memory_limit>1</min_allocation_size_to_throw_on_memory_limit></clickhouse>",
    )
    user = "context_setup_" + uuid4().hex
    with node.with_replace_config(
        "/etc/clickhouse-server/config.d/async_metrics_no.xml",
        config,
        reload_before=True,
        reload_after=True,
    ):
        node.query(
            f"CREATE USER {user} SETTINGS log_comment='" + "x" * (8 * 1024 * 1024) + "'",
            settings={"max_query_size": 16 * 1024 * 1024},
        )
        try:
            node.query(f"GRANT SELECT ON *.* TO {user}")
            error = node.query_and_get_error(
                "SELECT 1",
                user=user,
                settings={
                    "log_comment": "",
                    "max_memory_usage": 4 * 1024 * 1024,
                    "max_untracked_memory": 0,
                },
            )
            assert "Received exception from server" in error, error
            assert "MEMORY_LIMIT_EXCEEDED" in error, error
            assert "Query memory limit exceeded during query setup" in error, error
            assert node.query("SELECT 1", user=user) == "1\n"
        finally:
            node.query(f"DROP USER {user}")
