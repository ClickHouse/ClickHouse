import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_server_fail_on_invalid_replicated_user(started_cluster):
    node1.query("CREATE USER test_invalid_user IDENTIFIED WITH no_password")

    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.users WHERE name = 'test_invalid_user'",
        "test_invalid_user\n",
    )

    zk = cluster.get_kazoo_client("zoo1")

    user_uuid = zk.get("/clickhouse/access/U/test_invalid_user")[0].decode("utf-8")
    entity_path = f"/clickhouse/access/uuid/{user_uuid}"

    original_data = zk.get(entity_path)[0].decode("utf-8")
    assert "ATTACH USER test_invalid_user" in original_data

    node2.stop_clickhouse()

    invalid_definition = """ATTACH USER test_invalid_user IDENTIFIED WITH no_password; ATTACH GRANT WRONG GRANT ON default.foo TO test_invalid_user;"""
    zk.set(entity_path, invalid_definition.encode("utf-8"))

    node2.start_clickhouse(start_wait_sec=60, expected_to_fail=True)

    assert node2.contains_in_log("SYNTAX_ERROR") or node2.contains_in_log(
        "Syntax error"
    ), "Expected SYNTAX_ERROR in logs"
