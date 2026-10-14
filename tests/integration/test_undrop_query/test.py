import logging
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["configs/with_delay_config.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def create_and_drop(name):
    table_uuid = str(uuid.uuid1())
    logging.info(f"{name} uuid: {table_uuid}")
    node.query(
        f"CREATE TABLE {name} UUID '{table_uuid}' (id Int32) ENGINE = MergeTree() ORDER BY id;"
    )
    node.query(f"DROP TABLE {name};")
    return table_uuid


def test_undrop_drop_and_undrop_loop(started_cluster):
    # Dropped first, so that its 20 s delay runs out while the other tables are undropped.
    expired_uuid = create_and_drop("test_undrop_expired")
    in_drop_queue = (
        f"SELECT count() FROM system.dropped_tables WHERE uuid = '{expired_uuid}'"
    )
    assert node.query(in_drop_queue) == "1\n"

    # Each table is undropped 0, 5 and 10 seconds after its own DROP.
    for i, delay in enumerate([0, 5, 10]):
        table_uuid = create_and_drop(f"test_undrop_{i}")
        time.sleep(delay)
        node.query(f"UNDROP TABLE test_undrop_{i} UUID '{table_uuid}';")

    # A table removed from the drop queue cannot be undropped.
    assert_eq_with_retry(node, in_drop_queue, "0", retry_count=120, sleep_time=0.5)
    error = node.query_and_get_error(
        f"UNDROP TABLE test_undrop_expired UUID '{expired_uuid}';"
    )
    assert "UNKNOWN_TABLE" in error
