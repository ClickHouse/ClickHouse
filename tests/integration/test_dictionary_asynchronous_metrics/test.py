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

    rows = node.query(
        "SELECT name, value, length(description) "
        f"FROM system.asynchronous_metrics WHERE name IN ({in_list}) "
        "ORDER BY name FORMAT TabSeparated"
    ).strip().splitlines()
    values = {n: float(v) for n, v, _ in (r.split("\t") for r in rows)}
    description_lengths = [int(d) for _, _, d in (r.split("\t") for r in rows)]

    assert len(values) == 7
    assert all(v >= 0 for v in values.values())
    assert all(d >= 10 for d in description_lengths)

    # Functional checks against the live dictionary we just created.
    assert values["DictionaryTotalCount"] >= 1
    assert values["DictionaryLoadedCount"] >= 1
    assert values["DictionaryTotalElementCount"] >= 3
    assert values["DictionaryTotalBytesAllocated"] > 0
    assert values["DictionaryMaxQueryCount"] >= 1

    node.query("DROP DICTIONARY d_agg;")
    node.query("DROP TABLE t_agg;")
