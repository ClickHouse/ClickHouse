import logging
import os
import random
import string

import pytest

from helpers.cluster import ClickHouseCluster

from pathlib import Path


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "test_local_storage", main_configs=["configs/config.xml"], stay_alive=True
        )

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def process_result(line: str):
    return sorted(
        list(
            map(
                lambda x: (int(x.split("\t")[0]), x.split("\t")[1]),
                filter(lambda x: len(x) > 0, line.split("\n")),
            )
        )
    )


def test_local_engine(started_cluster):
    node = started_cluster.instances["test_local_storage"]
    node.query(
        """
            CREATE TABLE test_0 (
                id Int64,
                data String
            ) ENGINE=Local('/data/example.csv', 'CSV');
        """
    )

    node.query(
        """
            INSERT INTO test_0 VALUES (1, '3'), (-1, '7'), (4, 'abc');
        """
    )

    result = node.query(
        """
            select * from test_0;
        """
    )

    assert [(-1, "7"), (1, "3"), (4, "abc")] == process_result(result)

    error_got = node.query_and_get_error(
        """
            INSERT INTO test_0 VALUES (5, 'arr'), (9, 'ty'), (0, '15');
        """
    )

    node.query(
        """
            SET engine_file_truncate_on_insert = 1;
        """
    )

    node.query(
        """
            INSERT INTO test_0 VALUES (5, 'arr'), (9, 'ty'), (0, '15');
        """,
        settings={"engine_file_truncate_on_insert": 1},
    )

    result = node.query(
        """
            SELECT * FROM test_0;
        """
    )

    assert [(0, "15"), (5, "arr"), (9, "ty")] == process_result(result)

    node.query(
        """
            SET local_create_new_file_on_insert = 1;
        """
    )

    node.query(
        """
            INSERT INTO test_0 VALUES (1, '3'), (-1, '7'), (4, 'abc');
        """,
        settings={"local_create_new_file_on_insert": 1},
    )

    result = node.query(
        """
            SELECT * FROM test_0;
        """
    )

    assert [
        (-1, "7"),
        (0, "15"),
        (1, "3"),
        (4, "abc"),
        (5, "arr"),
        (9, "ty"),
    ] == process_result(result)

    node.restart_clickhouse()

    result = node.query(
        """
            SELECT * FROM test_0;
        """
    )

    assert [(0, "15"), (5, "arr"), (9, "ty")] == process_result(result)


def test_table_function(started_cluster):
    node = started_cluster.instances["test_local_storage"]

    node.copy_file_to_container(
        "test_local_storage/files/example2.csv", "/data/example2.csv"
    )

    result = node.query(
        """
            SELECT * FROM local('/data/example2.csv', 'CSV', 'id Int64, data String');
        """
    )

    print("Res5", result)

    assert [(1, "Str1"), (2, "Str2")] == process_result(result)

    # assert False
