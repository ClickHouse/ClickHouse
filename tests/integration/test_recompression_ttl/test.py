import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/background_pool_config.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/background_pool_config.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def wait_part_in_parts(node, table, part_name, retries=40):
    for i in range(retries):
        result = node.query("SELECT name FROM system.parts where name = '{}' and table = '{}'".format(part_name, table))
        if result:
            return True
        time.sleep(0.5)
    else:
        return False


def optimize_final_table_until_success(node, table_name, retries=40):
    for i in range(retries):
        try:
            node.query("OPTIMIZE TABLE {} FINAL".format(table_name), settings={"optimize_throw_if_noop": "1"})
            return True
        except:
            time.sleep(0.5)
    else:
        return False


def wait_part_and_get_compression_codec(node, table, part_name, retries=40):
    if wait_part_in_parts(node, table, part_name, retries):
        return node.query(
            "SELECT default_compression_codec FROM system.parts where name = '{}' and table = '{}'".format(part_name,
                                                                                                           table)).strip()
    return None


def test_recompression_simple(started_cluster):
    node1.query(
        "CREATE TABLE table_for_recompression (d DateTime, key UInt64, data String) ENGINE MergeTree() ORDER BY tuple() TTL d + INTERVAL 10 SECOND RECOMPRESS CODEC(ZSTD(10)) SETTINGS merge_with_recompression_ttl_timeout = 0")
    node1.query("INSERT INTO table_for_recompression VALUES (now(), 1, '1')")

    assert node1.query("SELECT default_compression_codec FROM system.parts where name = 'all_1_1_0'") == "LZ4\n"

    codec = wait_part_and_get_compression_codec(node1, "table_for_recompression", "all_1_1_1")
    if not codec:
        assert False, "Part all_1_1_1 doesn't appeared in system.parts"

    assert codec == "ZSTD(10)"

    if wait_part_in_parts(node1, "table_for_recompression", "all_1_1_2", retries=20):
        assert False, "Redundant merge were assigned for part all_1_1_1 -> all_1_1_2"

    optimize_final_table_until_success(node1, "table_for_recompression")

    assert node1.query("SELECT default_compression_codec FROM system.parts where name = 'all_1_1_2'") == "ZSTD(10)\n"


def test_recompression_multiple_ttls(started_cluster):
    node2.query("CREATE TABLE table_for_recompression (d DateTime, key UInt64, data String) ENGINE MergeTree() ORDER BY tuple() \
    TTL d + INTERVAL 5 SECOND RECOMPRESS CODEC(ZSTD(10)), \
    d + INTERVAL 10 SECOND RECOMPRESS CODEC(ZSTD(11)), \
    d + INTERVAL 15 SECOND RECOMPRESS CODEC(ZSTD(12)) SETTINGS merge_with_recompression_ttl_timeout = 0")

    node2.query("INSERT INTO table_for_recompression VALUES (now(), 1, '1')")

    assert node2.query("SELECT default_compression_codec FROM system.parts where name = 'all_1_1_0'") == "LZ4\n"

    codec = wait_part_and_get_compression_codec(node2, "table_for_recompression", "all_1_1_1")
    if not codec:
        assert False, "Part all_1_1_1 doesn't appeared in system.parts"

    assert codec == "ZSTD(10)"

    codec = wait_part_and_get_compression_codec(node2, "table_for_recompression", "all_1_1_2")
    if not codec:
        assert False, "Part all_1_1_2 doesn't appeared in system.parts"

    assert codec == "ZSTD(11)"

    codec = wait_part_and_get_compression_codec(node2, "table_for_recompression", "all_1_1_3")
    if not codec:
        assert False, "Part all_1_1_3 doesn't appeared in system.parts"

    assert codec == "ZSTD(12)"

    if wait_part_in_parts(node2, "table_for_recompression", "all_1_1_4", retries=20):
        assert False, "Redundant merge were assigned for part all_1_1_3 -> all_1_1_4"

    optimize_final_table_until_success(node2, "table_for_recompression")

    assert node2.query("SELECT default_compression_codec FROM system.parts where name = 'all_1_1_4'") == "ZSTD(12)\n"

    assert node2.query(
        "SELECT recompression_ttl_info.expression FROM system.parts where name = 'all_1_1_4'") == "['plus(d, toIntervalSecond(10))','plus(d, toIntervalSecond(15))','plus(d, toIntervalSecond(5))']\n"


def test_recompression_replicated(started_cluster):
    for i, node in enumerate([node1, node2]):
        node.query("CREATE TABLE recompression_replicated (d DateTime, key UInt64, data String) \
        ENGINE ReplicatedMergeTree('/test/rr', '{}') ORDER BY tuple() \
        TTL d + INTERVAL 10 SECOND RECOMPRESS CODEC(ZSTD(13)) SETTINGS merge_with_recompression_ttl_timeout = 0".format(
            i + 1))

    node1.query("INSERT INTO recompression_replicated VALUES (now(), 1, '1')")
    node2.query("SYSTEM SYNC REPLICA recompression_replicated", timeout=5)

    assert node1.query(
        "SELECT default_compression_codec FROM system.parts where name = 'all_0_0_0' and table = 'recompression_replicated'") == "LZ4\n"
    assert node2.query(
        "SELECT default_compression_codec FROM system.parts where name = 'all_0_0_0' and table = 'recompression_replicated'") == "LZ4\n"

    codec1 = wait_part_and_get_compression_codec(node1, "recompression_replicated", "all_0_0_1")
    if not codec1:
        assert False, "Part all_0_0_1 doesn't appeared in system.parts on node1"

    codec2 = wait_part_and_get_compression_codec(node2, "recompression_replicated", "all_0_0_1")
    if not codec2:
        assert False, "Part all_0_0_1 doesn't appeared in system.parts on node2"

    assert codec1 == "ZSTD(13)"
    assert codec2 == "ZSTD(13)"
