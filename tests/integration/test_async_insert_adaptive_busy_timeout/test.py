import copy
import logging
import random
import timeit
from itertools import repeat
from math import floor
from multiprocessing import Pool

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


node = cluster.add_instance(
    "node",
    main_configs=["configs/zookeeper_config.xml"],
    user_configs=[
        "configs/users.xml",
    ],
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


_query_settings = {"async_insert": 1, "wait_for_async_insert": 1}


def _generate_values(size, min_int, max_int, array_size_range):
    gen_tuple = lambda _min_int, _max_int, _array_size_range: (
        random.randint(_min_int, _max_int),
        [
            random.randint(_min_int, _max_int)
            for _ in range(random.randint(*_array_size_range))
        ],
    )

    return map(lambda _: gen_tuple(min_int, max_int, array_size_range), range(size))


def _insert_query(table_name, settings, *args, **kwargs):
    settings_s = ", ".join("{}={}".format(k, settings[k]) for k in settings)
    INSERT_QUERY = "INSERT INTO {} SETTINGS {} VALUES {}"
    node.query(
        INSERT_QUERY.format(
            table_name,
            settings_s,
            ", ".join(map(str, _generate_values(*args, **kwargs))),
        )
    )


def _insert_queries_sequentially(
    table_name, settings, iterations, max_values_size, array_size_range
):
    for iter in range(iterations):
        _insert_query(
            table_name,
            settings,
            random.randint(1, max_values_size),
            iter * max_values_size,
            (iter + 1) * max_values_size - 1,
            array_size_range,
        )


def _insert_queries_in_parallel(
    table_name, settings, thread_num, tasks, max_values_size, array_size_range
):
    sizes = [random.randint(1, max_values_size) for _ in range(tasks)]
    min_ints = [iter * max_values_size for iter in range(tasks)]
    max_ints = [(iter + 1) * max_values_size - 1 for iter in range(tasks)]
    with Pool(thread_num) as p:
        p.starmap(
            _insert_query,
            zip(
                repeat(table_name),
                repeat(settings),
                sizes,
                min_ints,
                max_ints,
                repeat(array_size_range),
            ),
        )


def test_with_merge_tree():
    table_name = "async_insert_mt_table"
    node.query(
        "CREATE TABLE {} (a UInt64, b Array(UInt64)) ENGINE=MergeTree() ORDER BY a".format(
            table_name
        )
    )

    _insert_queries_sequentially(
        table_name,
        _query_settings,
        iterations=10,
        max_values_size=1000,
        array_size_range=[10, 50],
    )

    node.query("DROP TABLE IF EXISTS {}".format(table_name))


def test_with_merge_tree_multithread():
    thread_num = 15
    table_name = "async_insert_mt_multithread_table"
    node.query(
        "CREATE TABLE {} (a UInt64, b Array(UInt64)) ENGINE=MergeTree() ORDER BY a".format(
            table_name
        )
    )

    _insert_queries_in_parallel(
        table_name,
        _query_settings,
        thread_num=15,
        tasks=100,
        max_values_size=1000,
        array_size_range=[10, 15],
    )

    node.query("DROP TABLE IF EXISTS {}".format(table_name))


def test_with_replicated_merge_tree():
    table_name = "async_insert_replicated_mt_table"

    create_query = " ".join(
        (
            "CREATE TABLE {} (a UInt64, b Array(UInt64))".format(table_name),
            "ENGINE=ReplicatedMergeTree('/clickhouse/tables/test/{}', 'node')".format(
                table_name
            ),
            "ORDER BY a",
        )
    )

    node.query(create_query)

    settings = _query_settings
    _insert_queries_sequentially(
        table_name,
        settings,
        iterations=10,
        max_values_size=1000,
        array_size_range=[10, 50],
    )

    node.query("DROP TABLE {} SYNC".format(table_name))


def test_with_replicated_merge_tree_multithread():
    thread_num = 15
    table_name = "async_insert_replicated_mt_multithread_table"

    create_query = " ".join(
        (
            "CREATE TABLE {} (a UInt64, b Array(UInt64))".format(table_name),
            "ENGINE=ReplicatedMergeTree('/clickhouse/tables/test/{}', 'node')".format(
                table_name
            ),
            "ORDER BY a",
        )
    )

    node.query(create_query)

    _insert_queries_in_parallel(
        table_name,
        _query_settings,
        thread_num=15,
        tasks=100,
        max_values_size=1000,
        array_size_range=[10, 15],
    )

    node.query("DROP TABLE {} SYNC".format(table_name))


# Ensure that the combined duration of inserts with adaptive timeouts is less than
# the combined duration for fixed timeouts.
def test_compare_sequential_inserts_durations_for_adaptive_and_fixed_async_timeouts():
    fixed_tm_table_name = "async_insert_mt_fixed_async_timeout"
    node.query(
        "CREATE TABLE {} (a UInt64, b Array(UInt64)) ENGINE=MergeTree() ORDER BY a".format(
            fixed_tm_table_name
        )
    )

    fixed_tm_settings = copy.copy(_query_settings)
    fixed_tm_settings["async_insert_use_adaptive_busy_timeout"] = 0
    fixed_tm_settings["async_insert_busy_timeout_ms"] = 100

    fixed_tm_run_duration = timeit.timeit(
        lambda: _insert_queries_sequentially(
            fixed_tm_table_name,
            fixed_tm_settings,
            iterations=50,
            max_values_size=1000,
            array_size_range=[10, 50],
        ),
        setup="pass",
        number=3,
    )

    node.query("DROP TABLE IF EXISTS {}".format(fixed_tm_table_name))

    logging.debug(
        "Run duration with fixed asynchronous timeout is {} seconds".format(
            fixed_tm_run_duration
        )
    )

    adaptive_tm_table_name = "async_insert_mt_adaptive_async_timeout"
    node.query(
        "CREATE TABLE {} (a UInt64, b Array(UInt64)) ENGINE=MergeTree() ORDER BY a".format(
            adaptive_tm_table_name
        )
    )

    adaptive_tm_settings = copy.copy(_query_settings)
    adaptive_tm_settings["async_insert_busy_timeout_min_ms"] = 10
    adaptive_tm_settings["async_insert_busy_timeout_max_ms"] = 500

    adaptive_tm_run_duration = timeit.timeit(
        lambda: _insert_queries_sequentially(
            adaptive_tm_table_name,
            adaptive_tm_settings,
            iterations=50,
            max_values_size=1000,
            array_size_range=[10, 50],
        ),
        setup="pass",
        number=3,
    )

    logging.debug(
        "Run duration with adaptive asynchronous timeout is {} seconds.".format(
            adaptive_tm_run_duration
        )
    )

    node.query("DROP TABLE IF EXISTS {}".format(adaptive_tm_table_name))

    assert adaptive_tm_run_duration <= fixed_tm_run_duration


# Ensure that the combined duration of inserts with adaptive timeouts is less than
# the combined duration for fixed timeouts.
def test_compare_parallel_inserts_durations_for_adaptive_and_fixed_async_timeouts():
    fixed_tm_table_name = "async_insert_mt_fixed_async_timeout"
    node.query(
        "CREATE TABLE {} (a UInt64, b Array(UInt64)) ENGINE=MergeTree() ORDER BY a".format(
            fixed_tm_table_name
        )
    )

    fixed_tm_settings = copy.copy(_query_settings)
    fixed_tm_settings["async_insert_use_adaptive_busy_timeout"] = 0
    fixed_tm_settings["async_insert_busy_timeout_ms"] = 500

    fixed_tm_run_duration = timeit.timeit(
        lambda: _insert_queries_in_parallel(
            fixed_tm_table_name,
            fixed_tm_settings,
            thread_num=15,
            tasks=150,
            max_values_size=1000,
            array_size_range=[10, 50],
        ),
        setup="pass",
        number=3,
    )

    node.query("DROP TABLE IF EXISTS {}".format(fixed_tm_table_name))

    logging.debug(
        "Run duration with fixed asynchronous timeout is {} seconds".format(
            fixed_tm_run_duration
        )
    )

    adaptive_tm_table_name = "async_insert_mt_adaptive_async_timeout"
    node.query(
        "CREATE TABLE {} (a UInt64, b Array(UInt64)) ENGINE=MergeTree() ORDER BY a".format(
            adaptive_tm_table_name
        )
    )

    adaptive_tm_settings = copy.copy(_query_settings)
    adaptive_tm_settings["async_insert_busy_timeout_min_ms"] = 10
    adaptive_tm_settings["async_insert_busy_timeout_max_ms"] = 500

    adaptive_tm_run_duration = timeit.timeit(
        lambda: _insert_queries_in_parallel(
            adaptive_tm_table_name,
            adaptive_tm_settings,
            thread_num=15,
            tasks=150,
            max_values_size=1000,
            array_size_range=[10, 50],
        ),
        setup="pass",
        number=3,
    )

    logging.debug(
        "Run duration with adaptive asynchronous timeout is {} seconds.".format(
            adaptive_tm_run_duration
        )
    )

    node.query("DROP TABLE IF EXISTS {}".format(adaptive_tm_table_name))

    assert adaptive_tm_run_duration <= fixed_tm_run_duration


# Ensure that the delay converges to a minimum for sequential inserts and wait_for_async_insert=1.
def test_change_queries_frequency():
    table_name = "async_insert_mt_change_queries_frequencies"

    create_query = " ".join(
        (
            "CREATE TABLE {} (a UInt64, b Array(UInt64))".format(table_name),
            "ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_frequencies/{}', 'node')".format(
                table_name
            ),
            "ORDER BY a",
        )
    )

    node.query(create_query)

    settings = copy.copy(_query_settings)
    min_ms = 50
    max_ms = 200

    settings["async_insert_busy_timeout_min_ms"] = min_ms
    settings["async_insert_busy_timeout_max_ms"] = max_ms

    _insert_queries_sequentially(
        table_name,
        settings,
        iterations=50,
        max_values_size=1000,
        array_size_range=[10, 50],
    )
    node.query("SYSTEM FLUSH LOGS")
    select_log_query = f"SELECT countIf(timeout_milliseconds - {min_ms} < 25) FROM (SELECT timeout_milliseconds FROM system.asynchronous_insert_log ORDER BY event_time DESC LIMIT 10)"
    res = node.query(select_log_query)
    assert int(res) >= 5

    _insert_queries_in_parallel(
        table_name,
        settings,
        thread_num=10,
        tasks=1000,
        max_values_size=1000,
        array_size_range=[10, 15],
    )
    node.query("SYSTEM FLUSH LOGS")
    select_log_query = f"SELECT countIf({max_ms} - timeout_milliseconds < 100) FROM (SELECT timeout_milliseconds FROM system.asynchronous_insert_log ORDER BY event_time DESC LIMIT 10)"
    res = node.query(select_log_query)
    assert int(res) >= 5

    node.query("DROP TABLE IF EXISTS {} SYNC".format(table_name))
