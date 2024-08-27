#!/usr/bin/env python3

import logging
import pytest
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node1", user_configs=["configs/users.xml"])

        cluster.add_instance(
            "node2",
            user_configs=[
                "configs/users.xml",
            ],
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_check_grant(cluster):
    node1 = cluster.instances["node1"]

    node1.query("DROP user  IF EXISTS tuser")
    node1.query("CREATE USER tuser")
    node1.query("GRANT SELECT ON tb TO tuser")
    # Has been granted but not table not exists
    res, _ = node1.query("CHECK GRANT SELECT ON tb", user="tuser")
    assert res == "0"

    node1.query(
        "CREATE TABLE tb (`content` UInt64) ENGINE = MergeTree ORDER BY content"
    )
    node1.query("INSERT INTO tb VALUES (1)")
    # Has been granted and table exists
    res, _ = node1.query("CHECK GRANT SELECT ON tb", user="tuser")
    assert res == "1"

    node1.query("REVOKE SELECT ON tb FROM tuser")
    # Has not been granted but table exists
    res, _ = node1.query("CHECK GRANT SELECT ON tb", user="tuser")
    assert res == "0"

    # Role
    node1.query("CREATE ROLE trole")
    node1.query("GRANT SELECT ON tb TO trole")
    node1.query("GRANT trole TO tuser")
    node1.query("SET ROLE trole", user="tuser")
    res, _ = node1.query("CHECK GRANT SELECT ON tb", user="tuser")
    assert res == "1"
