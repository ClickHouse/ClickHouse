import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
new_node = cluster.add_instance("new_node", with_zookeeper=False)
old_node = cluster.add_instance(
    "old_node",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


# Since 26.8 `arrayCount` returns `UInt64` (`UInt32` before). A new coordinator
# with `array_count_legacy_uint32_result = 1` (or `compatibility` <= '26.7')
# must be able to query a mix of old and new servers without a type mismatch:
# the coordinator builds a `UInt32` header, new servers compute `UInt32`
# because the setting is forwarded to them, and old servers compute `UInt32`
# natively while ignoring the unknown setting.
#
# An old coordinator must also be able to query the same mixed cluster: it
# resolves `arrayCount` as `UInt32` and does not know the setting, so the new
# server computes `UInt64` and the initiator converts the remote block to its
# `UInt32` header. That conversion is a modular cast — the same truncation the
# old implementation itself performed (it counted in `size_t` and narrowed
# with `static_cast<UInt32>`), so the old coordinator observes exactly the
# pre-upgrade semantics for arrays of any size.
def test_array_count_mixed_version_remote(start_cluster):
    for node in (new_node, old_node):
        node.query("CREATE TABLE tab (arr Array(UInt64)) ENGINE = Memory")
    new_node.query("INSERT INTO tab VALUES ([1, 2, 3])")
    old_node.query("INSERT INTO tab VALUES ([2, 3, 4, 5])")

    query = (
        "SELECT sum(c), any(toTypeName(c)) FROM "
        "(SELECT arrayCount(x -> x >= 2, arr) AS c "
        "FROM remote('old_node,new_node', default, tab))"
    )

    assert (
        new_node.query(query, settings={"array_count_legacy_uint32_result": 1})
        == "6\tUInt32\n"
    )
    assert (
        new_node.query(query, settings={"compatibility": CLICKHOUSE_CI_MIN_TESTED_VERSION})
        == "6\tUInt32\n"
    )

    # The old coordinator resolves `arrayCount` as `UInt32` and sends no
    # settings; the new server computes `UInt64` and the initiator converts it
    # to the `UInt32` header, matching the old (modular) semantics.
    assert old_node.query(query) == "6\tUInt32\n"

    # A type-sensitive wrapper executed on the shard, however, observes the
    # shard-local `arrayCount` type before the initiator can convert anything:
    # during a rolling upgrade an old coordinator sees `byteSize(arrayCount())`
    # jump from 4 to 8 on upgraded shards. This shard-local semantic shift is
    # inherent to any function-signature change guarded by a per-server
    # compatibility setting (an old coordinator cannot forward a setting it
    # does not know, and rejects it as unknown when set explicitly); the
    # mitigation is to set `array_count_legacy_uint32_result = 1` in the new
    # servers' default profile for the duration of the rolling upgrade.
    wrapped_query = (
        "SELECT byteSize(arrayCount(x -> x >= 2, arr)) AS b "
        "FROM remote('old_node,new_node', default, tab) ORDER BY b"
    )

    assert old_node.query(wrapped_query) == "4\n8\n"

    # From a new coordinator the setting is forwarded to new shards and
    # ignored by old ones, so the legacy behavior is uniform again.
    assert (
        new_node.query(
            wrapped_query,
            settings={"array_count_legacy_uint32_result": 1},
        )
        == "4\n4\n"
    )

    for node in (new_node, old_node):
        node.query("DROP TABLE tab")
