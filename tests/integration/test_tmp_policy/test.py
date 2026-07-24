# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_local = cluster.add_instance(
    "node_local",
    main_configs=["configs/config.d/storage_configuration.xml"],
    tmpfs=["/disk1:size=100M", "/disk2:size=100M"],
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
        "Setting up .*disk1.* to store temporary data in it"
    )
    assert node_local.contains_in_log(
        "Setting up .*disk2.* to store temporary data in it"
    )

    node_local.query(query, settings=settings)
    assert node_local.contains_in_log(
        "Writing part of aggregation data into temporary file.*/disk1/"
    )
    assert node_local.contains_in_log(
        "Writing part of aggregation data into temporary file.*/disk2/"
    )


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
