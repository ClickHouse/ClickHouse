import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/minio.xml"],
    stay_alive=True,
    with_minio=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_s3_storage_class_right(started_cluster):
    node.query(
        """
            CREATE TABLE test_s3_storage_class
            (
                `id` UInt64,
                `value` String
            )
            ENGINE = MergeTree
            ORDER BY id
            SETTINGS storage_policy='use_s3_storage_class';
        """,
    )
    node.query(
        """
            INSERT INTO test_s3_storage_class VALUES (1, 'a');
        """,
    )
    result = node.query(
        """
            SELECT id FROM test_s3_storage_class;
        """
    )

    assert result == "1\n"
