import pytest
import asyncio
import re
import random
import os.path
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", user_configs=["users.xml"])


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_create_query_constraints():

    expected_error = "Setting max_threads should not be changed"

    assert expected_error in instance.query_and_get_error(
        f"CREATE USER inner_user SETTINGS max_threads = 1"
    )

    assert expected_error in instance.query_and_get_error(
        f"CREATE USER inner_user SETTINGS max_threads MIN 0 MAX 2"
    )

    assert expected_error in instance.query_and_get_error(
        f"CREATE USER inner_user SETTINGS max_threads WRITABLE"
    )

    assert expected_error in instance.query_and_get_error(
        f"CREATE ROLE inner_role SETTINGS max_threads = 1"
    )

    assert expected_error in instance.query_and_get_error(
        f"CREATE ROLE inner_role SETTINGS max_threads = 1"
    )
