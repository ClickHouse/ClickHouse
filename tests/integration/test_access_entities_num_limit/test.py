import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node3",
    with_zookeeper=False,
    main_configs=["configs/config.xml"],
    stay_alive=True,
)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    macros={"replica": "r1"},
    main_configs=["configs/config.xml", "configs/config_replicated.xml"],
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    macros={"replica": "r2"},
    main_configs=["configs/config.xml", "configs/config_replicated.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_access_limit_default(started_cluster):
    roles = 5
    users = 5
    for i in range(roles):
        node.query("create role r{}".format(i))

    for i in range(users):
        node.query("create user u{}".format(i))

    assert "Too many access entities" in node.query_and_get_error("create user ux")

    # Updating existing users not changing user count
    for i in range(users):
        node.query("create user or replace u{}".format(i))

    for i in range(users):
        node.query("create user if not exists u{}".format(i))

    # Drop decreases the metric
    for i in range(users):
        node.query("drop user u{}".format(i))

    for i in range(users):
        node.query("create user u{}".format(i))

    assert "Too many access entities" in node.query_and_get_error("create user ux")

    # Loaded users are counted correctly
    node.restart_clickhouse()

    assert "Too many access entities" in node.query_and_get_error("create user ux")


def test_access_limit_replicated(started_cluster):
    roles = 5
    users = 5

    for i in range(roles):
        node1.query("create role r{}".format(i))

    for i in range(users):
        node1.query("create user u{}".format(i))

    assert "Too many access entities" in node1.query_and_get_error("create user ux")

    users -= 1
    node.query("drop user u{}".format(users))

    # Updating existing users not changing user count
    for i in range(users):
        node1.query("create user or replace u{}".format(i))

    for i in range(users):
        node1.query("create user if not exists u{}".format(i))

    # Drop decreases the metric
    for i in range(users):
        node1.query("drop user u{}".format(i))

    users += 1
    for i in range(users):
        node1.query("create user u{}".format(i))

    assert "Too many access entities" in node1.query_and_get_error("create user ux")

    # Loaded users are counted correctly
    node1.restart_clickhouse()

    assert "Too many access entities" in node1.query_and_get_error("create user ux")

    # Replica counts entities too
    assert "Too many access entities" in node2.query_and_get_error_with_retry(
        "create user ux"
    )

    for i in range(users):
        node1.query("drop user u{}".format(i))

    for i in range(users):
        node2.query("create user u{}".format(i))

    assert "Too many access entities" in node2.query_and_get_error("create user ux")
