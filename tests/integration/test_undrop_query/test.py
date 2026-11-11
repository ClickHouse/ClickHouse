import logging
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["configs/with_delay_config.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_undrop_drop_and_undrop_loop(started_cluster):
    uuid_list = []

    for i in range(4):
        table_uuid = uuid.uuid1().__str__()
        uuid_list.append(table_uuid)
        logging.info(f"table_uuid: {table_uuid}")

        node.query(
            f"CREATE TABLE test_undrop_{i} UUID '{table_uuid}' (id Int32) ENGINE = MergeTree() ORDER BY id;"
        )

        node.query(f"DROP TABLE test_undrop_{i};")

    for i in range(4):
        if (
            i >= 3
        ):  # First 3 tables are undropped after 0, 5 and 10 seconds. Fourth is undropped after 21 seconds
            time.sleep(6)
            error = node.query_and_get_error(
                f"UNDROP TABLE test_undrop_loop_{i} UUID '{uuid_list[i]}';"
            )
            assert "UNKNOWN_TABLE" in error
        else:
            node.query(f"UNDROP TABLE test_undrop_loop_{i} UUID '{uuid_list[i]}';")
            time.sleep(5)
