import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', image='yandex/clickhouse-server', tag='20.8.11.17', with_installed_binary=True, stay_alive=True)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()

def test_default_codec_read(start_cluster):
    node1.query("DROP TABLE IF EXISTS test_18340")

    node1.query("""
        CREATE TABLE test_18340
        (
            `lns` LowCardinality(Nullable(String)),
            `ns` Nullable(String),
            `s` String,
            `ni64` Nullable(Int64),
            `ui64` UInt64,
            `alns` Array(LowCardinality(Nullable(String))),
            `ans` Array(Nullable(String)),
            `dt` DateTime,
            `i32` Int32
        )
        ENGINE = MergeTree()
        PARTITION BY i32
        ORDER BY (s, farmHash64(s))
        SAMPLE BY farmHash64(s)
    """)

    node1.query("insert into test_18340 values ('test', 'test', 'test', 0, 0, ['a'], ['a'], now(), 0)")


    assert node1.query("SELECT COUNT() FROM test_18340") == "1\n"

    node1.restart_with_latest_version()

    assert node1.query("SELECT COUNT() FROM test_18340") == "1\n"
