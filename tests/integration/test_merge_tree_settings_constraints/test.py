import asyncio
import os.path
import random
import re

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", user_configs=["users.xml"])


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_merge_tree_settings_constraints():
    assert "Setting storage_policy should not be changed" in instance.query_and_get_error(
        f"CREATE TABLE wrong_table (number Int64) engine = MergeTree() ORDER BY number SETTINGS storage_policy = 'secret_policy'"
    )

    expected_error = "Setting min_bytes_for_wide_part should"

    assert expected_error in instance.query_and_get_error(
        f"CREATE TABLE wrong_table (number Int64) engine = MergeTree() ORDER BY number SETTINGS min_bytes_for_wide_part = 100"
    )

    assert expected_error in instance.query_and_get_error(
        f"CREATE TABLE wrong_table (number Int64) engine = MergeTree() ORDER BY number SETTINGS min_bytes_for_wide_part = 1000000000"
    )

    instance.query(
        f"CREATE TABLE good_table (number Int64) engine = MergeTree() ORDER BY number SETTINGS min_bytes_for_wide_part = 10000000"
    )

    assert expected_error in instance.query_and_get_error(
        f"ALTER TABLE good_table MODIFY SETTING min_bytes_for_wide_part = 100"
    )

    assert expected_error in instance.query_and_get_error(
        f"ALTER TABLE good_table MODIFY SETTING min_bytes_for_wide_part = 1000000000"
    )
