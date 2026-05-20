import pytest
import time

from helpers.cluster import ClickHouseCluster

HEAVY_MERTICS_CONFIG = [
    "configs/heavy_metrics.xml",
]

get_metrics_query = """
SELECT value FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalFailedUpdates';
SELECT value FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxUpdateDelay';
"""

check_delay_query = """
SELECT value FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalFailedUpdates';
SELECT if(value >= %s, 'ok', 'fail: ' || toString(value)) FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxUpdateDelay';
"""

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, main_configs=HEAVY_MERTICS_CONFIG)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_metrics(start_cluster):
    node.query("DROP DICTIONARY IF EXISTS d1;")
    node.query("DROP TABLE IF EXISTS t1;")
    node.query("CREATE TABLE t1 (key String, value String) ENGINE = MergeTree() PRIMARY KEY key;")
    node.query("CREATE DICTIONARY d1 (key String, value String) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE t1)) LAYOUT(FLAT()) LIFETIME(min 0 max 0);")

    node.query("SYSTEM RELOAD DICTIONARY d1;")
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS;")

    assert node.query(get_metrics_query) == "0\n0\n"

    node.query("DETACH TABLE t1;")

    for i in range(1, 6):
        node.query("SYSTEM RELOAD DICTIONARY d1;", ignore_error=True)

        time.sleep(1)

        node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS;")

        assert node.query(check_delay_query % i) == f"{i}\nok\n"

    node.query("ATTACH TABLE t1;")
    node.query("SYSTEM RELOAD DICTIONARY d1;")
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS;")

    assert node.query(get_metrics_query) == "0\n0\n"


def test_aggregate_metrics(start_cluster):
    """Regression test for gh-49182: aggregate dictionary metrics are emitted."""
    node.query("DROP DICTIONARY IF EXISTS d_agg;")
    node.query("DROP TABLE IF EXISTS t_agg;")
    node.query("CREATE TABLE t_agg (k UInt64, v String) ENGINE = Memory;")
    node.query("INSERT INTO t_agg VALUES (1, 'one'), (2, 'two'), (3, 'three');")
    node.query(
        "CREATE DICTIONARY d_agg (k UInt64, v String) PRIMARY KEY k "
        "SOURCE(CLICKHOUSE(TABLE 't_agg')) LAYOUT(HASHED()) LIFETIME(0);"
    )
    node.query("SYSTEM RELOAD DICTIONARY d_agg;")
    # One query so DictionaryMaxQueryCount becomes >= 1.
    assert node.query("SELECT dictGet('d_agg', 'v', toUInt64(2))").strip() == "two"
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS;")

    new_metrics = [
        "DictionaryFailedCount",
        "DictionaryLoadedCount",
        "DictionaryLoadingCount",
        "DictionaryMaxQueryCount",
        "DictionaryTotalBytesAllocated",
        "DictionaryTotalCount",
        "DictionaryTotalElementCount",
    ]
    in_list = ",".join(f"'{m}'" for m in new_metrics)

    # All 7 new metrics must be present.
    assert node.query(
        f"SELECT count() FROM system.asynchronous_metrics WHERE name IN ({in_list})"
    ).strip() == "7"

    # None of the Dictionary* metrics may report a negative value.
    assert node.query(
        "SELECT count() FROM system.asynchronous_metrics "
        "WHERE name LIKE 'Dictionary%' AND value < 0"
    ).strip() == "0"

    # Each new metric must have a non-empty documentation string.
    assert node.query(
        f"SELECT count() FROM system.asynchronous_metrics "
        f"WHERE name IN ({in_list}) AND length(description) < 10"
    ).strip() == "0"

    # Functional checks against the live dictionary we just created.
    assert node.query(
        "SELECT value >= 1 FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalCount'"
    ).strip() == "1"
    assert node.query(
        "SELECT value >= 1 FROM system.asynchronous_metrics WHERE name = 'DictionaryLoadedCount'"
    ).strip() == "1"
    assert node.query(
        "SELECT value >= 3 FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalElementCount'"
    ).strip() == "1"
    assert node.query(
        "SELECT value > 0 FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalBytesAllocated'"
    ).strip() == "1"
    assert node.query(
        "SELECT value >= 1 FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxQueryCount'"
    ).strip() == "1"

    node.query("DROP DICTIONARY d_agg;")
    node.query("DROP TABLE t_agg;")
