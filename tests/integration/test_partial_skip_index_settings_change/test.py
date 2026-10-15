import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/users.d/profile.xml"],
    stay_alive=True,
)

PROFILE = "/etc/clickhouse-server/users.d/profile.xml"
NAMED_OFF = "<enable_named_columns_in_function_tuple>0</enable_named_columns_in_function_tuple>"
NAMED_ON = "<enable_named_columns_in_function_tuple>1</enable_named_columns_in_function_tuple>"

# `i` holds the rows whose tuple is serialized as an object, `u` the ones serialized as an array.
QUERY_I = """SELECT count() FROM t WHERE id = 1 AND toJSONString(tuple(p)) = '{"p":1}'"""
QUERY_U = """SELECT count() FROM t WHERE id = 3 AND toJSONString(tuple(p)) = '[1]'"""
# `k` indexes the serialized tuple itself.
QUERY_K_OBJECT = """SELECT count() FROM t WHERE p > 0 AND toJSONString(tuple(p)) = '{"p":1}'"""
QUERY_K_ARRAY = """SELECT count() FROM t WHERE p > 0 AND toJSONString(tuple(p)) = '[1]'"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def counts(query):
    """The count without skip indexes, then with the index analysed before and during reading."""
    modes = [
        "use_skip_indexes = 0",
        "use_skip_indexes = 1, use_skip_indexes_on_data_read = 0",
        "use_skip_indexes = 1, use_skip_indexes_on_data_read = 1",
    ]
    return [node.query(f"{query} SETTINGS use_query_condition_cache = 0, {mode}").strip() for mode in modes]


def index_skips(query, index, rows=1):
    """Raises unless the index is used and leaves at most `rows` granules of one row to read."""
    return node.query(
        f"{query} SETTINGS use_query_condition_cache = 0, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, "
        f"force_data_skipping_indices = '{index}', max_rows_to_read = {rows}"
    ).strip()


def test_tuple_names_change_after_restart(started_cluster):
    node.replace_in_config(PROFILE, NAMED_ON, NAMED_OFF)
    node.restart_clickhouse()

    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        """
        CREATE TABLE t
        (
            id UInt64,
            p UInt8,
            INDEX i id TYPE minmax GRANULARITY 1 WHERE toJSONString(tuple(p)) = '{"p":1}',
            INDEX u id TYPE minmax GRANULARITY 1 WHERE toJSONString(tuple(p)) = '[1]',
            INDEX k toJSONString(tuple(p)) TYPE set(10) GRANULARITY 1 WHERE p > 0
        )
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS index_granularity = 1
        """
    )
    node.query("INSERT INTO t VALUES (1, 1), (2, 2), (3, 1)")

    # The tuple has no names: it is serialized as an array, so no row is in `i`.
    assert counts(QUERY_I) == ["0", "0", "0"]
    assert counts(QUERY_U) == ["1", "1", "1"]
    assert index_skips(QUERY_U, "u") == "1"
    assert counts(QUERY_K_OBJECT) == ["0", "0", "0"]
    assert index_skips(QUERY_K_ARRAY, "k", rows=2) == "2"

    # A restart with the same settings keeps the indexes usable.
    node.restart_clickhouse()
    assert counts(QUERY_U) == ["1", "1", "1"]
    assert index_skips(QUERY_U, "u") == "1"
    assert index_skips(QUERY_K_ARRAY, "k", rows=2) == "2"

    # The tuple has names now and the rows are serialized as objects. The granules of `i` and `k` were written for
    # the other type of the tuple and must not be read.
    node.replace_in_config(PROFILE, NAMED_OFF, NAMED_ON)
    node.restart_clickhouse()
    assert counts(QUERY_I) == ["1", "1", "1"]
    assert counts(QUERY_U) == ["0", "0", "0"]
    assert counts(QUERY_K_OBJECT) == ["2", "2", "2"]

    # Rebuilt for the current type, the index is read again.
    node.query("ALTER TABLE t MATERIALIZE INDEX i SETTINGS mutations_sync = 2")
    assert counts(QUERY_I) == ["1", "1", "1"]
    assert index_skips(QUERY_I, "i") == "1"

    node.query("DROP TABLE t SYNC")
    node.replace_in_config(PROFILE, NAMED_ON, NAMED_OFF)
    node.restart_clickhouse()
