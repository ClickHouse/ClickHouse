import logging

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


# This test construct intersecting parts intentially. It's not a elegent test.
# TODO(hanfei): write a test which select part 1_1 merging with part 2_2 and drop range.
def test_intersect_parts_when_restart(started_cluster):
    node.query(
        """
         CREATE TABLE data (
             key Int
         )
         ENGINE = ReplicatedMergeTree('/ch/tables/default/data', 'node')
         ORDER BY key;
         """
    )
    node.query("system stop cleanup data")
    node.query("INSERT INTO data values (1)")
    node.query("INSERT INTO data values (2)")
    node.query("INSERT INTO data values (3)")
    node.query("INSERT INTO data values (4)")
    node.query("ALTER TABLE data DROP PART 'all_1_1_0'")
    node.query("ALTER TABLE data DROP PART 'all_2_2_0'")
    node.query("OPTIMIZE TABLE data FINAL")

    part_path = node.query(
        "SELECT path FROM system.parts WHERE table = 'data' and name = 'all_0_3_1'"
    ).strip()

    assert len(part_path) != 0

    node.query("detach table data")
    new_path = part_path[:-6] + "1_2_3"
    node.exec_in_container(
        [
            "bash",
            "-c",
            "cp -r {p} {p1}".format(p=part_path, p1=new_path),
        ],
        privileged=True,
    )

    # mock empty part
    node.exec_in_container(
        [
            "bash",
            "-c",
            "echo -n 0 > {p1}/count.txt".format(p1=new_path),
        ],
        privileged=True,
    )

    node.query("attach table data")
    data_size = node.query("SELECT sum(key) FROM data").strip()
    assert data_size == "5"
