import asyncio
import os.path
import random
import re

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    user_configs=[
        "configs/users.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_create_query_const_constraints():
    instance.query("CREATE USER u_const SETTINGS max_threads = 1 CONST")
    instance.query("GRANT ALL ON *.* TO u_const")

    expected_error = "Setting max_threads should not be changed"

    assert expected_error in instance.query_and_get_error(
        "CREATE USER inner_user SETTINGS max_threads = 1", user="u_const"
    )
    assert expected_error in instance.query_and_get_error(
        "CREATE USER inner_user SETTINGS max_threads MIN 0 MAX 2", user="u_const"
    )
    assert expected_error in instance.query_and_get_error(
        "CREATE USER inner_user SETTINGS max_threads WRITABLE", user="u_const"
    )
    assert expected_error in instance.query_and_get_error(
        "CREATE ROLE inner_role SETTINGS max_threads = 1", user="u_const"
    )
    assert expected_error in instance.query_and_get_error(
        "CREATE SETTINGS PROFILE inner_profile SETTINGS max_threads = 1", user="u_const"
    )

    instance.query(
        "CREATE USER inner_user_1 SETTINGS max_threads CONST", user="u_const"
    )
    instance.query(
        "CREATE USER inner_user_2 SETTINGS max_threads = 1 CONST", user="u_const"
    )
    instance.query("DROP USER u_const, inner_user_1, inner_user_2")


def test_create_query_minmax_constraints():
    instance.query("CREATE USER u_minmax SETTINGS max_threads = 4 MIN 2 MAX 6")
    instance.query("GRANT ALL ON *.* TO u_minmax")

    expected_error = "Setting max_threads shouldn't be less than"

    assert expected_error in instance.query_and_get_error(
        "CREATE USER inner_user SETTINGS max_threads = 1", user="u_minmax"
    )
    assert expected_error in instance.query_and_get_error(
        "CREATE USER inner_user SETTINGS max_threads MIN 1 MAX 3", user="u_minmax"
    )
    assert expected_error in instance.query_and_get_error(
        "CREATE ROLE inner_role SETTINGS max_threads MIN 1 MAX 3", user="u_minmax"
    )
    assert expected_error in instance.query_and_get_error(
        "CREATE SETTINGS PROFILE inner_profile SETTINGS max_threads MIN 1 MAX 3",
        user="u_minmax",
    )

    expected_error = "Setting max_threads shouldn't be greater than"

    assert expected_error in instance.query_and_get_error(
        "CREATE USER inner_user SETTINGS max_threads = 8", user="u_minmax"
    )
    assert expected_error in instance.query_and_get_error(
        "CREATE USER inner_user SETTINGS max_threads MIN 4 MAX 8", user="u_minmax"
    )
    assert expected_error in instance.query_and_get_error(
        "CREATE ROLE inner_role SETTINGS max_threads MIN 4 MAX 8", user="u_minmax"
    )
    assert expected_error in instance.query_and_get_error(
        "CREATE SETTINGS PROFILE inner_profile SETTINGS max_threads MIN 4 MAX 8",
        user="u_minmax",
    )

    instance.query("CREATE USER inner_user SETTINGS max_threads = 3", user="u_minmax")
    instance.query("DROP USER u_minmax, inner_user")
