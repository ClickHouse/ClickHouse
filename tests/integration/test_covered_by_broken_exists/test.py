import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", stay_alive=True, with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)

instance = node1
q = node1.query

path_to_data = "/var/lib/clickhouse/"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_merged_part(table, part_name, retries=100):
    q("OPTIMIZE TABLE {} FINAL".format(table))
    for i in range(retries):
        result = q(
            "SELECT name FROM system.parts where table='{}' AND name='{}'".format(
                table, part_name
            )
        )
        if result:
            return True
        time.sleep(0.5)
    else:
        return False


def test_make_clone_covered_by_broken_detached_dir_exists(started_cluster):
    q("DROP TABLE IF EXISTS test_make_clone_cvbdde SYNC")

    q(
        "CREATE TABLE test_make_clone_cvbdde(n int, m String) ENGINE=ReplicatedMergeTree('/test_make_clone_cvbdde', '1') ORDER BY n SETTINGS old_parts_lifetime=3600, min_age_to_force_merge_seconds=1, min_age_to_force_merge_on_partition_only=0"
    )
    path = path_to_data + "data/default/test_make_clone_cvbdde/"

    q("INSERT INTO test_make_clone_cvbdde VALUES (0, 'hbl')")

    q("INSERT INTO test_make_clone_cvbdde VALUES (1, 'hbl')")
    if not (wait_merged_part("test_make_clone_cvbdde", "all_0_1_1")):
        assert False, "Part all_0_1_1 doesn't appeared in system.parts"

    q("INSERT INTO test_make_clone_cvbdde VALUES (2, 'hbl')")
    if not (wait_merged_part("test_make_clone_cvbdde", "all_0_2_2")):
        assert False, "Part all_0_2_2 doesn't appeared in system.parts"

    q("INSERT INTO test_make_clone_cvbdde VALUES (3, 'hbl')")
    if not (wait_merged_part("test_make_clone_cvbdde", "all_0_3_3")):
        assert False, "Part all_0_3_3 doesn't appeared in system.parts"

    res = str(instance.exec_in_container(["ls", path]).strip().split("\n"))

    # broke the merged parts
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'broken' > {}".format(path + "all_0_1_1/data.bin"),
        ]
    )

    instance.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'broken' > {}".format(path + "all_0_2_2/data.bin"),
        ]
    )

    instance.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'broken' > {}".format(path + "all_0_3_3/data.bin"),
        ]
    )

    instance.restart_clickhouse(kill=True)

    assert [
        "broken-on-start_all_0_1_1",
        "broken-on-start_all_0_2_2",
        "broken-on-start_all_0_3_3",
        "covered-by-broken_all_0_0_0",
        "covered-by-broken_all_1_1_0",
        "covered-by-broken_all_2_2_0",
        "covered-by-broken_all_3_3_0",
    ] == sorted(
        instance.exec_in_container(["ls", path + "detached/"]).strip().split("\n")
    )
