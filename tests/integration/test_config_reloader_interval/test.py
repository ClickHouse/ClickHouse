#!/usr/bin/env python3

import pytest
import fnmatch

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config_reloader.xml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_reload_config(start_cluster):
    assert node.wait_for_log_line(
        f"Config reload interval set to 1000ms", look_behind_lines=2000
    )

    node.replace_in_config(
        "/etc/clickhouse-server/config.d/config_reloader.xml",
        "1000",
        "7777",
    )

    assert node.wait_for_log_line(
        f"Config reload interval changed to 7777ms", look_behind_lines=2000
    )
