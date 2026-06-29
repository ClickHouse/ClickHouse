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
