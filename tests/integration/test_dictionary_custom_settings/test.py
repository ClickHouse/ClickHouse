import os

import pytest
from helpers.cluster import ClickHouseCluster

DICTIONARY_FILES = [
    "configs/dictionaries/FileSourceConfig.xml",
    "configs/dictionaries/ExecutableSourceConfig.xml",
    "configs/dictionaries/source.csv",
    "configs/dictionaries/HTTPSourceConfig.xml",
    "configs/dictionaries/ClickHouseSourceConfig.xml",
]

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("node", dictionaries=DICTIONARY_FILES)


def prepare():
    node = instance
    path = "/source.csv"

    script_dir = os.path.dirname(os.path.realpath(__file__))
    node.copy_file_to_container(
        os.path.join(script_dir, "./http_server.py"), "/http_server.py"
    )
    node.copy_file_to_container(
        os.path.join(script_dir, "configs/dictionaries/source.csv"), "./source.csv"
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            "python3 /http_server.py --data-path={tbl} --schema=http --host=localhost --port=5555".format(
                tbl=path
            ),
        ],
        detach=True,
    )


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        prepare()
        yield cluster
    finally:
        cluster.shutdown()


def test_work(start_cluster):
    query = instance.query

    instance.query("SYSTEM RELOAD DICTIONARIES")

    assert query("SELECT dictGetString('test_file', 'first', toUInt64(1))") == "\\'a\n"
    assert query("SELECT dictGetString('test_file', 'second', toUInt64(1))") == '"b\n'
    assert (
        query("SELECT dictGetString('test_executable', 'first', toUInt64(1))")
        == "\\'a\n"
    )
    assert (
        query("SELECT dictGetString('test_executable', 'second', toUInt64(1))")
        == '"b\n'
    )

    caught_exception = ""
    try:
        instance.query(
            "CREATE TABLE source (id UInt64, first String, second String, third String) ENGINE=TinyLog;"
        )
        instance.query(
            "INSERT INTO default.source VALUES (1, 'aaa', 'bbb', 'cccc'), (2, 'ddd', 'eee', 'fff')"
        )
        instance.query("SELECT dictGetString('test_clickhouse', 'second', toUInt64(1))")
    except Exception as e:
        caught_exception = str(e)

    assert caught_exception.find("Limit for result exceeded") != -1

    assert query("SELECT dictGetString('test_http', 'first', toUInt64(1))") == "\\'a\n"
    assert query("SELECT dictGetString('test_http', 'second', toUInt64(1))") == '"b\n'
