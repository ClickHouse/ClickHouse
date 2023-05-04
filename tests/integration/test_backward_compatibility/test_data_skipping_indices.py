# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    image="yandex/clickhouse-server",
    tag="21.6",
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


# TODO: cover other types too, but for this we need to add something like
# restart_with_tagged_version(), since right now it is not possible to
# switch to old tagged clickhouse version.
def test_index(start_cluster):
    node.query(
        """
    CREATE TABLE data
    (
        key Int,
        value Nullable(Int),
        INDEX value_index value TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY key;

    INSERT INTO data SELECT number, number FROM numbers(10000);

    SELECT * FROM data WHERE value = 20000 SETTINGS force_data_skipping_indices = 'value_index' SETTINGS force_data_skipping_indices = 'value_index', max_rows_to_read=1;
    """
    )
    node.restart_with_latest_version()
    node.query(
        """
    SELECT * FROM data WHERE value = 20000 SETTINGS force_data_skipping_indices = 'value_index' SETTINGS force_data_skipping_indices = 'value_index', max_rows_to_read=1;
    DROP TABLE data;
    """
    )
