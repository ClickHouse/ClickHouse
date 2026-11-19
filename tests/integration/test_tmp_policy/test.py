# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_local = cluster.add_instance(
    "node_local",
    main_configs=["configs/config.d/storage_configuration.xml"],
    tmpfs=["/test_tmp_policy_disk1:size=100M", "/test_tmp_policy_disk2:size=100M"],
    stay_alive=True,
)

node_remote = cluster.add_instance(
    "node_remote",
    main_configs=["configs/config.d/remote_storage_configuration.xml"],
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_multiple_local_disk():
    query = "SELECT count(ignore(*)) FROM (SELECT * FROM system.numbers LIMIT 1e7) GROUP BY number"
    settings = {
        "max_bytes_ratio_before_external_group_by": 0,
        "max_bytes_ratio_before_external_sort": 0,
        "max_bytes_before_external_group_by": 1 << 20,
        "max_bytes_before_external_sort": 1 << 20,
    }

    assert node_local.contains_in_log(
        "Setting up temporary data storage at disk 'disk1'"
    )
    assert node_local.contains_in_log(
        "Setting up temporary data storage at disk 'disk2'"
    )

    node_local.query(query, settings=settings)
    assert node_local.contains_in_log(
        "Writing part of aggregation data into temporary file.*/test_tmp_policy_disk1/"
    )
    assert node_local.contains_in_log(
        "Writing part of aggregation data into temporary file.*/test_tmp_policy_disk2/"
    )


def test_multiple_local_disk_distinct():
    # The query has no `ORDER BY` / `GROUP BY`, so the temporary-file log lines below can only come from the
    # external `DISTINCT` spill.
    query = "SELECT count() FROM (SELECT DISTINCT number FROM numbers(1e7))"
    settings = {
        "max_bytes_ratio_before_external_distinct": 0,
        "max_bytes_before_external_distinct": 1 << 20,
        "max_untracked_memory": 0,
    }

    node_local.query(query, settings=settings)
    assert node_local.contains_in_log(
        "Writing part of data into temporary file.*/test_tmp_policy_disk1/"
    )
    assert node_local.contains_in_log(
        "Writing part of data into temporary file.*/test_tmp_policy_disk2/"
    )


def test_multiple_local_disk_full_disk():
    # With `disk1` nearly full, every spill of the external sort reserves its bytes on `disk2`. The threshold
    # of the external sort is split among the 16 sorting threads: 32 KiB each, less than a block, so every
    # thread spills every block it sorts. The threads reserve at the same time, and a reservation must try
    # every disk of the volume even when the other threads reserve between its attempts.
    filler = "/test_tmp_policy_disk1/filler"
    node_local.exec_in_container(
        [
            "bash",
            "-c",
            f"fallocate -l $(( $(df --output=avail -B1 /test_tmp_policy_disk1 | tail -1) - 8192 )) {filler}",
        ]
    )
    try:
        query_id = str(uuid.uuid4())
        settings = {
            "max_threads": 16,
            "max_threads_min_free_memory_per_thread": 0,
            "max_block_size": 8192,
            "max_bytes_ratio_before_external_sort": 0,
            "max_bytes_before_external_sort": 1 << 19,
        }
        node_local.query(
            "SELECT number FROM numbers_mt(2000000) ORDER BY number DESC FORMAT Null",
            settings=settings,
            query_id=query_id,
        )
        node_local.query("SYSTEM FLUSH LOGS query_log")
        assert node_local.query(
            "SELECT ProfileEvents['ExternalSortWritePart'] > 100 "
            f"FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        ) == "1\n"
    finally:
        node_local.exec_in_container(["rm", "-f", filler])


def test_remote_disk():
    query = "SELECT count(ignore(*)) FROM (SELECT * FROM system.numbers LIMIT 1e7) GROUP BY number"
    settings = {
        "max_bytes_ratio_before_external_group_by": 0,
        "max_bytes_ratio_before_external_sort": 0,
        "max_bytes_before_external_group_by": 1 << 20,
        "max_bytes_before_external_sort": 1 << 20,
    }

    node_remote.query(query, settings=settings)
    assert node_remote.contains_in_log(
        "Writing part of aggregation data into temporary file.*disk_s3_plain"
    )
    assert node_remote.contains_in_log(
        "Writing part of aggregation data into temporary file.*disk_s3_plain"
    )


def test_remote_disk_distinct():
    # The query has no `ORDER BY` / `GROUP BY`, so the temporary-file log line below can only come from the
    # external `DISTINCT` spill.
    query = "SELECT count() FROM (SELECT DISTINCT number FROM numbers(1e7))"
    settings = {
        "max_bytes_ratio_before_external_distinct": 0,
        "max_bytes_before_external_distinct": 1 << 20,
        "max_untracked_memory": 0,
    }

    node_remote.query(query, settings=settings)
    assert node_remote.contains_in_log(
        "Writing part of data into temporary file.*disk_s3_plain"
    )


# The set of `IN` spills to disk once it takes 1 MiB: its sorter writes the keys as runs to temporary
# files and merges them into the finished set, and every block of rows on the left side looks its keys up
# in the blocks of that file.
IN_QUERY = "SELECT count() FROM numbers(1e6) WHERE number IN (SELECT number * 3 FROM numbers(2e6))"
IN_SETTINGS = {
    "max_bytes_ratio_before_external_set": 0,
    "max_bytes_before_external_set": 1 << 20,
    "max_untracked_memory": 0,
}


def assert_set_read_from_disk(node, query_id):
    node.query("SYSTEM FLUSH LOGS query_log")
    assert node.query(
        "SELECT ProfileEvents['SetsSpilledToDisk'], ProfileEvents['ExternalSetReadBlocks'] > 0 "
        f"FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    ) == "1\t1\n"


def test_multiple_local_disk_in():
    query_id = str(uuid.uuid4())
    assert node_local.query(IN_QUERY, settings=IN_SETTINGS, query_id=query_id) == "333334\n"
    assert node_local.contains_in_log(
        f"{{{query_id}}}.*Wrote .* keys of a set of IN to temporary file disk(disk1)"
    )
    assert node_local.contains_in_log(
        f"{{{query_id}}}.*Wrote .* keys of a set of IN to temporary file disk(disk2)"
    )
    assert node_local.contains_in_log(
        f"{{{query_id}}}.*Created set on disk with 2000000 keys .* in temporary file disk(disk[12])"
    )
    assert_set_read_from_disk(node_local, query_id)


def test_remote_disk_in():
    # The lookups read the blocks of the set at their offsets in the file on the remote disk.
    query_id = str(uuid.uuid4())
    assert node_remote.query(IN_QUERY, settings=IN_SETTINGS, query_id=query_id) == "333334\n"
    assert node_remote.contains_in_log(
        f"{{{query_id}}}.*Wrote .* keys of a set of IN to temporary file disk(disk_s3_plain)"
    )
    assert node_remote.contains_in_log(
        f"{{{query_id}}}.*Created set on disk with 2000000 keys .* in temporary file disk(disk_s3_plain)"
    )
    assert_set_read_from_disk(node_remote, query_id)


@pytest.mark.parametrize(
    "node, config, disk",
    [
        pytest.param(
            node_local,
            "storage_configuration.xml",
            "disk1",
            id="local_disk1",
        ),
        pytest.param(
            node_local,
            "storage_configuration.xml",
            "disk2",
            id="local_disk2",
        ),
        pytest.param(
            node_remote,
            "remote_storage_configuration.xml",
            "disk_s3_plain",
            id="remote",
        ),
    ],
)
def test_cleanup_temporary_disk_at_server_start(node, config, disk):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse disks -C /etc/clickhouse-server/config.d/{config} --disk {disk} -q 'write --path-to foo' <<<foo",
        ]
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse disks -C /etc/clickhouse-server/config.d/{config} --disk {disk} -q 'write --path-to tmpfoo' <<<foo",
        ]
    )
    assert node.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse disks -C /etc/clickhouse-server/config.d/{config} --disk {disk} -q 'ls'",
        ]
    ).strip().split("\n") == ["foo", "tmpfoo"]

    # Now restart the server to cleanup the tmp disk on start
    node.restart_clickhouse()

    assert node.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse disks -C /etc/clickhouse-server/config.d/{config} --disk {disk} -q 'ls'",
        ]
    ).strip().split("\n") == ["foo"]
