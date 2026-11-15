import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config_throw_on_invalid.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config_throw_on_invalid.xml"],
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
    user_name = f"test_invalid_user_{uuid.uuid4().hex[:8]}"

    node1.query(f"CREATE USER {user_name} IDENTIFIED WITH no_password")

    assert_eq_with_retry(
        node2,
        f"SELECT name FROM system.users WHERE name = '{user_name}'",
        f"{user_name}\n",
        retry_count=30,
        sleep_time=1,
    )

    zk = cluster.get_kazoo_client("zoo1")

    user_uuid = zk.get(f"/clickhouse/access/U/{user_name}")[0].decode("utf-8")
    entity_path = f"/clickhouse/access/uuid/{user_uuid}"

    original_data = zk.get(entity_path)[0].decode("utf-8")
    assert f"ATTACH USER {user_name}" in original_data

    node2.stop_clickhouse()

    invalid_definition = f"ATTACH USER {user_name} IDENTIFIED WITH no_password; ATTACH GRANT WRONG GRANT ON default.foo TO {user_name};"
    zk.set(entity_path, invalid_definition.encode("utf-8"))

    node2.start_clickhouse(start_wait_sec=60, expected_to_fail=True)

    assert node2.contains_in_log("SYNTAX_ERROR") or node2.contains_in_log(
        "Syntax error"
    ), "Expected SYNTAX_ERROR in logs"

    # Cleanup: restore valid entity and restart node2 for subsequent test runs
    zk.set(entity_path, original_data.encode("utf-8"))
    node2.start_clickhouse()
    node1.query(f"DROP USER {user_name}")
