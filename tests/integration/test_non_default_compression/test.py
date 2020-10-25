import time
import pytest
import string
import random

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/zstd_compression_by_default.xml'], user_configs=['configs/allow_suspicious_codecs.xml'])
node2 = cluster.add_instance('node2', main_configs=['configs/lz4hc_compression_by_default.xml'], user_configs=['configs/allow_suspicious_codecs.xml'])
node3 = cluster.add_instance('node3', main_configs=['configs/custom_compression_by_default.xml'], user_configs=['configs/allow_suspicious_codecs.xml'])
node4 = cluster.add_instance('node4', user_configs=['configs/enable_uncompressed_cache.xml', 'configs/allow_suspicious_codecs.xml'])
node5 = cluster.add_instance('node5', main_configs=['configs/zstd_compression_by_default.xml'], user_configs=['configs/enable_uncompressed_cache.xml', 'configs/allow_suspicious_codecs.xml'])

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_preconfigured_default_codec(start_cluster):
    for node in [node1, node2]:
        node.query("""
        CREATE TABLE compression_codec_multiple_with_key (
            somedate Date CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12)),
            id UInt64 CODEC(LZ4, ZSTD, NONE, LZ4HC),
            data String CODEC(ZSTD(2), LZ4HC, NONE, LZ4, LZ4),
            somecolumn Float64
        ) ENGINE = MergeTree() PARTITION BY somedate ORDER BY id SETTINGS index_granularity = 2;
        """)
        node.query("INSERT INTO compression_codec_multiple_with_key VALUES(toDate('2018-10-12'), 100000, 'hello', 88.88), (toDate('2018-10-12'), 100002, 'world', 99.99), (toDate('2018-10-12'), 1111, '!', 777.777)")
        assert node.query("SELECT COUNT(*) FROM compression_codec_multiple_with_key WHERE id % 2 == 0") == "2\n"
        assert node.query("SELECT DISTINCT somecolumn FROM compression_codec_multiple_with_key ORDER BY id") == "777.777\n88.88\n99.99\n"
        assert node.query("SELECT data FROM compression_codec_multiple_with_key WHERE id >= 1112 AND somedate = toDate('2018-10-12') AND somecolumn <= 100") == "hello\nworld\n"

        node.query("INSERT INTO compression_codec_multiple_with_key SELECT toDate('2018-10-12'), number, toString(number), 1.0 FROM system.numbers LIMIT 10000")

        assert node.query("SELECT COUNT(id) FROM compression_codec_multiple_with_key WHERE id % 10 == 0") == "1001\n"
        assert node.query("SELECT SUM(somecolumn) FROM compression_codec_multiple_with_key") == str(777.777 + 88.88 + 99.99 + 1.0 * 10000) + "\n"
        assert node.query("SELECT count(*) FROM compression_codec_multiple_with_key GROUP BY somedate") == "10003\n"

def test_preconfigured_custom_codec(start_cluster):
    node3.query("""
    CREATE TABLE compression_codec_multiple_with_key (
        somedate Date CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12)),
        id UInt64 CODEC(LZ4, ZSTD, NONE, LZ4HC),
        data String,
        somecolumn Float64 CODEC(ZSTD(2), LZ4HC, NONE, NONE, NONE, LZ4HC(5))
    ) ENGINE = MergeTree() PARTITION BY somedate ORDER BY id SETTINGS index_granularity = 2;
    """)

    node3.query("INSERT INTO compression_codec_multiple_with_key VALUES(toDate('2018-10-12'), 100000, 'hello', 88.88), (toDate('2018-10-12'), 100002, 'world', 99.99), (toDate('2018-10-12'), 1111, '!', 777.777)")
    assert node3.query("SELECT COUNT(*) FROM compression_codec_multiple_with_key WHERE id % 2 == 0") == "2\n"
    assert node3.query("SELECT DISTINCT somecolumn FROM compression_codec_multiple_with_key ORDER BY id") == "777.777\n88.88\n99.99\n"
    assert node3.query("SELECT data FROM compression_codec_multiple_with_key WHERE id >= 1112 AND somedate = toDate('2018-10-12') AND somecolumn <= 100") == "hello\nworld\n"

    node3.query("INSERT INTO compression_codec_multiple_with_key VALUES(toDate('2018-10-12'), 100000, '{}', 88.88)".format(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10000))))

    node3.query("OPTIMIZE TABLE compression_codec_multiple_with_key FINAL")
    assert node3.query("SELECT max(length(data)) from compression_codec_multiple_with_key GROUP BY data ORDER BY max(length(data)) DESC LIMIT 1") == "10000\n"

    for i in xrange(10):
        node3.query("INSERT INTO compression_codec_multiple_with_key VALUES(toDate('2018-10-12'), {}, '{}', 88.88)".format(i, ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10000))))

    node3.query("OPTIMIZE TABLE compression_codec_multiple_with_key FINAL")

    assert node3.query("SELECT COUNT(*) from compression_codec_multiple_with_key WHERE length(data) = 10000") == "11\n"

def test_uncompressed_cache_custom_codec(start_cluster):
    node4.query("""
    CREATE TABLE compression_codec_multiple_with_key (
        somedate Date CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12)),
        id UInt64 CODEC(LZ4, ZSTD, NONE, LZ4HC),
        data String,
        somecolumn Float64 CODEC(ZSTD(2), LZ4HC, NONE, NONE, NONE, LZ4HC(5))
    ) ENGINE = MergeTree() PARTITION BY somedate ORDER BY id SETTINGS index_granularity = 2;
    """)

    node4.query("INSERT INTO compression_codec_multiple_with_key VALUES(toDate('2018-10-12'), 100000, '{}', 88.88)".format(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10000))))

    # two equal requests one by one, to get into UncompressedCache for the first block
    assert node4.query("SELECT max(length(data)) from compression_codec_multiple_with_key GROUP BY data ORDER BY max(length(data)) DESC LIMIT 1") == "10000\n"

    assert node4.query("SELECT max(length(data)) from compression_codec_multiple_with_key GROUP BY data ORDER BY max(length(data)) DESC LIMIT 1") == "10000\n"

def test_uncompressed_cache_plus_zstd_codec(start_cluster):
    node5.query("""
    CREATE TABLE compression_codec_multiple_with_key (
        somedate Date CODEC(ZSTD, ZSTD, ZSTD(12), LZ4HC(12)),
        id UInt64 CODEC(LZ4, ZSTD, NONE, LZ4HC),
        data String,
        somecolumn Float64 CODEC(ZSTD(2), LZ4HC, NONE, NONE, NONE, LZ4HC(5))
    ) ENGINE = MergeTree() PARTITION BY somedate ORDER BY id SETTINGS index_granularity = 2;
    """)

    node5.query("INSERT INTO compression_codec_multiple_with_key VALUES(toDate('2018-10-12'), 100000, '{}', 88.88)".format('a' * 10000))

    assert node5.query("SELECT max(length(data)) from compression_codec_multiple_with_key GROUP BY data ORDER BY max(length(data)) DESC LIMIT 1") == "10000\n"
