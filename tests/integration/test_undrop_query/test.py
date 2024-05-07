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
    node.query("set allow_experimental_undrop_table_query = 1;")
    while count < 10:
        random_sec = random.randint(0, 10)
        table_uuid = uuid.uuid1().__str__()
        logging.info(
            "random_sec: " + random_sec.__str__() + ", table_uuid: " + table_uuid
        )
        node.query(
            "create table test_undrop_loop"
            + count.__str__()
            + " UUID '"
            + table_uuid
            + "' (id Int32) Engine=MergeTree() order by id;"
        )
        node.query("drop table test_undrop_loop" + count.__str__() + ";")
        time.sleep(random_sec)
        if random_sec >= 5:
            error = node.query_and_get_error(
                "undrop table test_undrop_loop"
                + count.__str__()
                + " uuid '"
                + table_uuid
                + "' settings allow_experimental_undrop_table_query = 1;"
            )
            assert "UNKNOWN_TABLE" in error
        else:
            node.query(
                "undrop table test_undrop_loop"
                + count.__str__()
                + " uuid '"
                + table_uuid
                + "' settings allow_experimental_undrop_table_query = 1;"
            )
            count = count + 1
