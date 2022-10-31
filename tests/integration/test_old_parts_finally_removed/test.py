#!/usr/bin/env python3

import os
import time

import helpers.client as client
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_part_finally_removed(started_cluster):
    node1.query(
        "CREATE TABLE drop_outdated_part (Key UInt64) ENGINE = ReplicatedMergeTree('/table/d', '1') ORDER BY tuple() SETTINGS old_parts_lifetime=10, cleanup_delay_period=10, cleanup_delay_period_random_add=1"
    )
    node1.query("INSERT INTO drop_outdated_part VALUES (1)")

    node1.query("OPTIMIZE TABLE drop_outdated_part FINAL")

    node1.exec_in_container(
        [
            "bash",
            "-c",
            "chown -R root:root /var/lib/clickhouse/data/default/drop_outdated_part/all_0_0_0",
        ],
        privileged=True,
        user="root",
    )

    node1.query(
        "ALTER TABLE drop_outdated_part MODIFY SETTING old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=1"
    )

    for i in range(60):
        if node1.contains_in_log("Cannot quickly remove directory"):
            break
        time.sleep(0.5)

    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"chown -R {os.getuid()}:{os.getgid()} /var/lib/clickhouse/data/default/drop_outdated_part/*",
        ],
        privileged=True,
        user="root",
    )

    for i in range(60):

        if (
            node1.query(
                "SELECT count() from system.parts WHERE table = 'drop_outdated_part'"
            )
            == "1\n"
        ):
            out = node1.exec_in_container(
                [
                    "bash",
                    "-c",
                    "ls /var/lib/clickhouse/data/default/drop_outdated_part | grep '_' | grep 'all'",
                ],
                privileged=True,
                user="root",
            )

            assert (
                "all_0_0_0" not in out
            ), f"Print found part which must be deleted: {out}"

            break
        time.sleep(0.5)
