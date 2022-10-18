#!/usr/bin/env python3
# pylint: disable=unused-argument

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "storage_policy,to_disk",
    [
        pytest.param(
            "default",
            "default",
            id="from_local_to_local",
        ),
        pytest.param(
            "s3",
            "default",
            id="from_s3_to_local",
        ),
        pytest.param(
            "default",
            "s3",
            id="from_local_to_s3",
        ),
        pytest.param(
            "s3",
            "s3_plain",
            id="from_s3_to_s3_plain",
        ),
        pytest.param(
            "default",
            "s3_plain",
            id="from_local_to_s3_plain",
        ),
    ],
)
def test_backup_restore(start_cluster, storage_policy, to_disk):
    backup_name = storage_policy + "_" + to_disk
    node.query(
        f"""
    DROP TABLE IF EXISTS data NO DELAY;
    CREATE TABLE data (key Int, value String, array Array(String)) Engine=MergeTree() ORDER BY tuple() SETTINGS storage_policy='{storage_policy}';
    INSERT INTO data SELECT * FROM generateRandom('key Int, value String, array Array(String)') LIMIT 1000;
    BACKUP TABLE data TO Disk('{to_disk}', '{backup_name}');
    RESTORE TABLE data AS data_restored FROM Disk('{to_disk}', '{backup_name}');
    SELECT throwIf(
        (SELECT groupArray(tuple(*)) FROM data) !=
        (SELECT groupArray(tuple(*)) FROM data_restored),
        'Data does not matched after BACKUP/RESTORE'
    );
    DROP TABLE data NO DELAY;
    DROP TABLE data_restored NO DELAY;
    """
    )
