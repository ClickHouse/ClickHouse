#!/usr/bin/env python3

import os
import time

import pytest

import helpers.client as client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import get_retry_number

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_part_finally_removed(started_cluster, request):
    node1.query("DROP TABLE IF EXISTS drop_outdated_part")
    retry = get_retry_number(request)
    node1.query(
        f"""
        CREATE TABLE drop_outdated_part (Key UInt64) ENGINE = ReplicatedMergeTree('/table/d{retry}', '1') ORDER BY tuple()
        SETTINGS old_parts_lifetime=10, cleanup_delay_period=10, cleanup_delay_period_random_add=1, cleanup_thread_preferred_points_per_iteration=0
        """
    )
    node1.query("INSERT INTO drop_outdated_part VALUES (1)")

    node1.query("OPTIMIZE TABLE drop_outdated_part FINAL")

    data_path = node1.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='drop_outdated_part'"
    ).strip()

    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"chown -R root:root {data_path}/all_0_0_0",
        ],
        privileged=True,
        user="root",
    )

    node1.query(
        "ALTER TABLE drop_outdated_part MODIFY SETTING old_parts_lifetime=1, cleanup_delay_period=1, cleanup_delay_period_random_add=1, cleanup_thread_preferred_points_per_iteration=0"
    )

    for i in range(60):
        if node1.contains_in_log("Cannot quickly remove directory"):
            break
        time.sleep(0.5)

    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"chown -R {os.getuid()}:{os.getgid()} {data_path}/*",
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
                    f"ls {data_path} | grep '_' | grep 'all'",
                ],
                privileged=True,
                user="root",
            )

            assert (
                "all_0_0_0" not in out
            ), f"Print found part which must be deleted: {out}"

            break
        time.sleep(0.5)

    node1.query("DROP TABLE drop_outdated_part")
