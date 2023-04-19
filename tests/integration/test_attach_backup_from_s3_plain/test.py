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
    set allow_deprecated_database_ordinary=1;

    create database ordinary_db engine=Ordinary;

    create table ordinary_db.{table_name} engine=MergeTree() order by key partition by part as select number%5 part, number key from numbers(100);
    -- NOTE: name of backup ("backup") is significant.
    backup table ordinary_db.{table_name} TO Disk('backup_disk_s3_plain', '{backup_name}') settings deduplicate_files=0;

    drop table ordinary_db.{table_name};
    attach table ordinary_db.{table_name} (part UInt8, key UInt64)
    engine=MergeTree()
    order by key partition by part
    settings
        min_bytes_for_wide_part={min_bytes_for_wide_part},
        max_suspicious_broken_parts=0,
        storage_policy='{storage_policy}';
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
