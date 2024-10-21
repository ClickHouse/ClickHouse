import os

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

from .generate_dictionaries import (
    generate_structure,
    generate_dictionaries,
    DictionaryTestTable,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = None
instance = None
test_table = None


def setup_module(module):
    global cluster
    global instance
    global test_table

    structure = generate_structure()
    dictionary_files = generate_dictionaries(
        os.path.join(SCRIPT_DIR, "configs/dictionaries"), structure
    )

    cluster = ClickHouseCluster(__file__)
    instance = cluster.add_instance("instance", dictionaries=dictionary_files)
    test_table = DictionaryTestTable(
        os.path.join(SCRIPT_DIR, "configs/dictionaries/source.tsv")
    )


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        test_table.create_clickhouse_source(instance)
        for line in TSV(instance.query("select name from system.dictionaries")).lines:
            print(line, end=" ")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(
    params=[
        # name, keys, use_parent
        ("clickhouse_hashed", ("id",), True),
        ("clickhouse_flat", ("id",), True),
        ("clickhouse_complex_integers_key_hashed", ("key0", "key1"), False),
        ("clickhouse_complex_mixed_key_hashed", ("key0_str", "key1"), False),
        ("clickhouse_range_hashed", ("id", "StartDate", "EndDate"), False),
    ],
    ids=[
        "clickhouse_hashed",
        "clickhouse_flat",
        "clickhouse_complex_integers_key_hashed",
        "clickhouse_complex_mixed_key_hashed",
        "clickhouse_range_hashed",
    ],
)
def dictionary_structure(started_cluster, request):
    return request.param


def test_select_all(dictionary_structure):
    name, keys, use_parent = dictionary_structure
    query = instance.query

    structure = test_table.get_structure_for_keys(keys, use_parent)
    query(
        """
    DROP TABLE IF EXISTS test.{0}
    """.format(
            name
        )
    )

    create_query = "CREATE TABLE test.{0} ({1}) engine = Dictionary({0})".format(
        name, structure
    )
    TSV(query(create_query))

    result = TSV(query("select * from test.{0}".format(name)))

    diff = test_table.compare_by_keys(
        keys, result.lines, use_parent, add_not_found_rows=True
    )
    print(test_table.process_diff(diff))
    assert not diff


@pytest.fixture(
    params=[
        # name, keys, use_parent
        ("clickhouse_cache", ("id",), True),
        ("clickhouse_complex_integers_key_cache", ("key0", "key1"), False),
        ("clickhouse_complex_mixed_key_cache", ("key0_str", "key1"), False),
    ],
    ids=[
        "clickhouse_cache",
        "clickhouse_complex_integers_key_cache",
        "clickhouse_complex_mixed_key_cache",
    ],
)
def cached_dictionary_structure(started_cluster, request):
    return request.param


def test_select_all_from_cached(cached_dictionary_structure):
    name, keys, use_parent = cached_dictionary_structure
    query = instance.query

    structure = test_table.get_structure_for_keys(keys, use_parent)
    query(
        """
    DROP TABLE IF EXISTS test.{0}
    """.format(
            name
        )
    )

    create_query = "CREATE TABLE test.{0} ({1}) engine = Dictionary({0})".format(
        name, structure
    )
    TSV(query(create_query))

    for i in range(4):
        result = TSV(query("select * from test.{0}".format(name)))
        diff = test_table.compare_by_keys(
            keys, result.lines, use_parent, add_not_found_rows=False
        )
        print(test_table.process_diff(diff))
        assert not diff

        key = []
        for key_name in keys:
            if key_name.endswith("str"):
                key.append("'" + str(i) + "'")
            else:
                key.append(str(i))
        if len(key) == 1:
            key = "toUInt64(" + str(i) + ")"
        else:
            key = str("(" + ",".join(key) + ")")
        query("select dictGetUInt8('{0}', 'UInt8_', {1})".format(name, key))

    result = TSV(query("select * from test.{0}".format(name)))
    diff = test_table.compare_by_keys(
        keys, result.lines, use_parent, add_not_found_rows=True
    )
    print(test_table.process_diff(diff))
    assert not diff
