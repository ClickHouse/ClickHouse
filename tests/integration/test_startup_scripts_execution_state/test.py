import random
import string
import time
from enum import Enum

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
good = cluster.add_instance(
    "good",
    main_configs=["config/users.xml", "config/good_script.xml"],
    stay_alive=True,
)
bad = cluster.add_instance(
    "bad",
    main_configs=["config/users.xml", "config/bad_script.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_startup_execution_state(start_cluster):
    """
    Making sure that the StartupScriptsExecutionState metric is set correctly.
    """

    STATE_SUCCESS = 1
    STATE_FAILURE = 2

    assert (
        int(
            good.query(
                "SELECT value FROM system.metrics WHERE metric = 'StartupScriptsExecutionState'"
            ).strip()
        )
        == STATE_SUCCESS
    )

    assert (
        int(
            bad.query(
                "SELECT value FROM system.metrics WHERE metric = 'StartupScriptsExecutionState'"
            ).strip()
        )
        == STATE_FAILURE
    )
