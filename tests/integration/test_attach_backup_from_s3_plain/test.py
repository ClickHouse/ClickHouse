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


@pytest.mark.parametrize(
    "table_name,backup_name,storage_policy,min_bytes_for_wide_part",
    [
        pytest.param(
            "compact", "backup_compact", "s3_backup_compact", int(1e9), id="compact"
        ),
        pytest.param("wide", "backup_wide", "s3_backup_wide", int(0), id="wide"),
    ],
)
def test_attach_part(table_name, backup_name, storage_policy, min_bytes_for_wide_part):
    node.query(
        f"""
    -- Catch any errors (NOTE: warnings are ok)
    set send_logs_level='error';

    -- BACKUP writes Ordinary like structure
    -- but what is more important that you cannot ATTACH directly from Atomic
    -- so Memory engine will work (to avoid using allow_deprecated_database_ordinary).
    create database ordinary_db engine=Memory;

    create table ordinary_db.{table_name} engine=MergeTree() order by key partition by part
    settings min_bytes_for_wide_part={min_bytes_for_wide_part}
    as select number%5 part, number key from numbers(100);

    backup table ordinary_db.{table_name} TO Disk('backup_disk_s3_plain', '{backup_name}') settings deduplicate_files=0;

    drop table ordinary_db.{table_name};
    attach table ordinary_db.{table_name} (part UInt8, key UInt64)
    engine=MergeTree()
    order by key partition by part
    settings
        max_suspicious_broken_parts=0,
        disk=disk(type=s3_plain,
            endpoint='http://minio1:9001/root/data/disks/disk_s3_plain/{backup_name}/',
            access_key_id='minio',
            secret_access_key='minio123');
    """
    )

    assert int(node.query(f"select count() from ordinary_db.{table_name}")) == 100

    node.query(
        f"""
    -- NOTE: DROP DATABASE cannot be done w/o this due to metadata leftovers
    set force_remove_data_recursively_on_drop=1;
    drop database ordinary_db sync;
    """
    )
