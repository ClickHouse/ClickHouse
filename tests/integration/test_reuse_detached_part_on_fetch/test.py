import os
import shlex

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
source = cluster.add_instance("source", with_zookeeper=True)
destination = cluster.add_instance("destination", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def table_path(node, table="t"):
    return node.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()


def successful_fetches(node):
    return int(
        node.query(
            "SELECT sum(value) FROM system.events WHERE event = 'ReplicatedPartFetches'"
        )
    )


def test_new_replica_reuses_matching_detached_part(started_cluster):
    source.query(
        "CREATE TABLE t (x UInt64) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/reuse_detached_on_fetch', 'source') ORDER BY x"
    )
    source.query("SYSTEM STOP MERGES t")
    source.query("INSERT INTO t VALUES (111)")
    source.query("INSERT INTO t VALUES (222)")
    source.query("INSERT INTO t VALUES (333)")
    source.query("CREATE TABLE checksum_source (x UInt64) ENGINE = MergeTree ORDER BY x")
    source.query("INSERT INTO checksum_source VALUES (444)")

    part_names = source.query(
        "SELECT name FROM system.parts WHERE database = 'default' AND table = 't' AND active ORDER BY name"
    ).split()
    assert len(part_names) == 3

    source.query("SYSTEM STOP REPLICATED SENDS t")
    destination.query(
        "CREATE TABLE t (x UInt64) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/reuse_detached_on_fetch', 'destination') ORDER BY x"
    )
    destination.query("SYSTEM STOP FETCHES t")

    detached_path = os.path.join(table_path(destination), "detached")
    destination.exec_in_container(
        ["bash", "-c", f"mkdir -p {shlex.quote(detached_path)}"],
        privileged=True,
        user="root",
    )
    for part_name in part_names:
        cluster.copy_file_from_container_to_container(
            source,
            os.path.join(table_path(source), part_name),
            destination,
            detached_path + "/",
        )

    # The second candidate has a different `checksums.txt` and must be fetched instead.
    other_part_name = source.query(
        "SELECT name FROM system.parts WHERE database = 'default' AND table = 'checksum_source' AND active"
    ).strip()
    cluster.copy_file_from_container_to_container(
        source,
        os.path.join(table_path(source, "checksum_source"), other_part_name, "checksums.txt"),
        destination,
        os.path.join(detached_path, part_names[1], "checksums.txt"),
    )
    destination.exec_in_container(
        [
            "bash",
            "-c",
            f"chown -R clickhouse:clickhouse {shlex.quote(detached_path)}",
        ],
        privileged=True,
        user="root",
    )

    # Keep the third candidate's `checksums.txt` intact but corrupt its data bytes.
    destination.exec_in_container(
        [
            "python3",
            "-c",
            "from pathlib import Path\n"
            f"part_dir = Path({os.path.join(detached_path, part_names[2])!r})\n"
            "part_file = next(part_dir.glob('*.bin'))\n"
            "with part_file.open('r+b') as file:\n"
            "    byte = file.read(1)\n"
            "    assert byte\n"
            "    file.seek(0)\n"
            "    file.write(bytes([byte[0] ^ 1]))\n",
        ],
        privileged=True,
        user="root",
    )

    fetches_before = successful_fetches(destination)
    source.query("SYSTEM START REPLICATED SENDS t")
    destination.query("SYSTEM START FETCHES t")
    destination.query("SYSTEM SYNC REPLICA t")

    assert destination.query("SELECT x FROM t ORDER BY x") == "111\n222\n333\n"
    assert destination.query(
        "SELECT name FROM system.parts WHERE database = 'default' AND table = 't' AND active ORDER BY name"
    ).split() == part_names
    assert successful_fetches(destination) - fetches_before == 2
    assert destination.query(
        "SELECT name FROM system.detached_parts WHERE database = 'default' AND table = 't' ORDER BY name"
    ).split() == [part_names[1], f"broken_{part_names[2]}"]
