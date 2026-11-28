import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
new_node = cluster.add_instance("new_node", with_zookeeper=False)
# The setting was introduced in 26.11, so this peer must reject it rather than
# silently execute the legacy truth table during a rolling upgrade.
old_node = cluster.add_instance(
    "old_node",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag="26.10",
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


def test_old_peer_rejects_new_in_semantics(start_cluster):
    for node in (new_node, old_node):
        node.query("CREATE TABLE compat_t (k UInt8) ENGINE = Memory")
        node.query("INSERT INTO compat_t VALUES (0), (1), (2)")

    query = (
        "SELECT count() FROM remote('{peer}:9000', default, compat_t) "
        "WHERE k NOT IN (1, NULL)"
    )

    # The new peer executes the new truth table when the setting is forwarded.
    assert new_node.query(
        query.format(peer="new_node"), settings={"in_null_as_unknown": 1}
    ).strip() == "0"

    # The old peer can execute legacy semantics when the setting is disabled.
    assert new_node.query(query.format(peer="old_node")).strip() == "2"

    error = new_node.query_and_get_error(
        query.format(peer="old_node"), settings={"in_null_as_unknown": 1}
    )
    assert "UNKNOWN_SETTING" in error
    assert "in_null_as_unknown" in error
