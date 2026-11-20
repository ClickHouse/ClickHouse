import concurrent.futures
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


STATUS_METRICS_QUERY = (
    "SELECT name, value FROM system.asynchronous_metrics "
    "WHERE name IN ('DictionaryLoadedCount', 'DictionaryFailedCount', 'DictionaryLoadingCount') "
    "FORMAT TabSeparated"
)


def parse_status_metrics(rows):
    return {n: int(float(v)) for n, v in (r.split("\t") for r in rows)}


def get_status_metrics():
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS;")
    return parse_status_metrics(node.query(STATUS_METRICS_QUERY).strip().splitlines())


def dictionary_status_query(name):
    return f"SELECT status FROM system.dictionaries WHERE database = currentDatabase() AND name = '{name}'"


def get_dictionary_status(name):
    return node.query(dictionary_status_query(name)).strip()


def test_status_counts(start_cluster):
    node.query("DROP DICTIONARY IF EXISTS d_fail;")
    node.query("DROP DICTIONARY IF EXISTS d_slow;")

    baseline = get_status_metrics()

    # A dictionary whose initial load fails is counted in `DictionaryFailedCount`.
    node.query(
        "CREATE DICTIONARY d_fail (k UInt64, v String) PRIMARY KEY k "
        "SOURCE(CLICKHOUSE(TABLE 't_fail_missing_source')) LAYOUT(HASHED()) LIFETIME(0);"
    )
    node.query("SYSTEM RELOAD DICTIONARY d_fail;", ignore_error=True)
    assert get_dictionary_status("d_fail") == "FAILED"
    metrics = get_status_metrics()
    assert metrics["DictionaryFailedCount"] == baseline["DictionaryFailedCount"] + 1
    assert metrics["DictionaryLoadedCount"] == baseline["DictionaryLoadedCount"]
    assert metrics["DictionaryLoadingCount"] == baseline["DictionaryLoadingCount"]
    node.query("DROP DICTIONARY d_fail;")

    # A dictionary whose initial load is in progress is counted in `DictionaryLoadingCount`.
    def create_slow_dictionary():
        node.query("DROP DICTIONARY IF EXISTS d_slow;")
        node.query(
            "CREATE DICTIONARY d_slow (k UInt64, v String) PRIMARY KEY k "
            "SOURCE(CLICKHOUSE(QUERY 'SELECT number AS k, toString(number) AS v FROM numbers(1) WHERE sleep(3) = 0')) "
            "LAYOUT(HASHED()) LIFETIME(0);"
        )

    def metrics_during_load(in_progress_status):
        """The status metrics computed while `d_slow` is in `in_progress_status`, or None if its load ended first; a failed reload raises its own exception."""
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            reload = executor.submit(node.query, "SYSTEM RELOAD DICTIONARY d_slow;")
            while get_dictionary_status("d_slow") != in_progress_status:
                if reload.done():
                    reload.result()
                    return None
                time.sleep(0.05)
            # One client call, so that the status is read right after the metrics are computed.
            *rows, status = node.query(
                f"SYSTEM RELOAD ASYNCHRONOUS METRICS; {STATUS_METRICS_QUERY}; {dictionary_status_query('d_slow')};"
            ).strip().splitlines()
            reload.result()
        return parse_status_metrics(rows) if status == in_progress_status else None

    def check_during_load(in_progress_status, expected_delta, prepare=lambda: None):
        # A slow client can miss the 3 s load; such a measurement is taken again.
        for _ in range(5):
            prepare()
            metrics = metrics_during_load(in_progress_status)
            if metrics is not None:
                break
        else:
            pytest.fail(f"the metrics were never computed while d_slow was {in_progress_status}")
        for name, delta in expected_delta.items():
            assert metrics[name] == baseline[name] + delta, name

    check_during_load(
        "LOADING",
        {"DictionaryLoadingCount": 1, "DictionaryLoadedCount": 0, "DictionaryFailedCount": 0},
        prepare=create_slow_dictionary,
    )
    assert get_dictionary_status("d_slow") == "LOADED"

    # Reloading an already loaded dictionary keeps it in `DictionaryLoadedCount`.
    check_during_load(
        "LOADED_AND_RELOADING",
        {"DictionaryLoadingCount": 0, "DictionaryLoadedCount": 1, "DictionaryFailedCount": 0},
    )

    node.query("DROP DICTIONARY d_slow;")
