import pytest
import uuid
import random
import logging
import time

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
    count = 0
    while count < 10:
        random_sec = random.randint(0, 10)
        table_uuid = uuid.uuid1().__str__()
        logging.info(
            "random_sec: " + random_sec.__str__() + ", table_uuid: " + table_uuid
        )

        node.query(
            "CREATE TABLE test_undrop_loop"
            + count.__str__()
            + " UUID '"
            + table_uuid
            + "' (id Int32) ENGINE = MergeTree() ORDER BY id;"
        )

        node.query("DROP TABLE test_undrop_loop" + count.__str__() + ";")

        time.sleep(random_sec)

        if random_sec >= 5:
            error = node.query_and_get_error(
                "UNDROP TABLE test_undrop_loop"
                + count.__str__()
                + " UUID '"
                + table_uuid
                + "';"
            )
            assert "UNKNOWN_TABLE" in error
        elif random_sec <= 3:
            # (*)
            node.query(
                "UNDROP TABLE test_undrop_loop"
                + count.__str__()
                + " UUID '"
                + table_uuid
                + "';"
            )
            count = count + 1
        else:
            pass
            # ignore random_sec = 4 to account for communication delay with the database.
            # if we don't do that, then the second case (*) may find the table already dropped and receive an unexpected exception from the database (Bug #55167)
