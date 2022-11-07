# pylint: disable=global-statement
# pylint: disable=line-too-long

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/disk_s3.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def test_attach_backup():
    node.query(
        f"""
    -- BACKUP writes Ordinary like structure
    set allow_deprecated_database_ordinary=1;
    create database ordinary engine=Ordinary;

    create table ordinary.test_backup_attach engine=MergeTree() order by tuple() as select * from numbers(100);
    -- NOTE: name of backup ("backup") is significant.
    backup table ordinary.test_backup_attach TO Disk('backup_disk_s3_plain', 'backup');

    drop table ordinary.test_backup_attach;
    attach table ordinary.test_backup_attach (number UInt64) engine=MergeTree() order by tuple() settings storage_policy='attach_policy_s3_plain';
    """
    )

    assert int(node.query("select count() from ordinary.test_backup_attach")) == 100
