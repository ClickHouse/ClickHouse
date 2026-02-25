import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .common import verify_no_warning, verify_warning_with_values

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node3",
    with_zookeeper=False,
    main_configs=["config/access/config.xml"],
    stay_alive=True,
)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    macros={"replica": "r1"},
    main_configs=["config/access/config.xml", "config/access/replicated.xml"],
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    macros={"replica": "r2"},
    main_configs=["config/access/config.xml", "config/access/replicated.xml"],
    stay_alive=True,
)

limit = 10
warn_limit = 5
get_metric_query = "select value from system.metrics where metric = 'AccessEntities'"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def clear(node, users, roles):
    for i in range(users):
        node.query("drop user if exists u{}".format(i))
    for i in range(roles):
        node.query("drop role if exists r{}".format(i))


def get_metric_value(node):
    return int(node.query(get_metric_query))


def assert_correct_metric(node):
    assert "Too many access entities" in node.query_and_get_error("create user ux")
    assert limit == get_metric_value(node)


def test_access_limit_default(started_cluster):
    # Some users and profiles from config are already counted
    default_count = get_metric_value(node)
    roles = 3
    users = limit - roles - default_count

    assert users > 0

    for i in range(roles):
        node.query("create role r{}".format(i))

    for i in range(users):
        node.query("create user u{}".format(i))

    assert_correct_metric(node)

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

    assert_correct_metric(node)

    # Loaded users are counted correctly
    node.restart_clickhouse()

    assert_correct_metric(node)

    clear(node, users, roles)


def test_access_limit_replicated(started_cluster):
    users = limit - get_metric_value(node1)

    for i in range(users):
        node1.query("create user u{}".format(i))

    assert_correct_metric(node1)

    users -= 1
    node1.query("drop user u{}".format(users))

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

    assert_correct_metric(node1)

    # Loaded users are counted correctly
    node1.restart_clickhouse()

    assert_correct_metric(node1)

    # Replica counts entities too
    assert_eq_with_retry(node2, get_metric_query, f"{limit}\n")
    assert_correct_metric(node2)

    for i in range(users):
        node1.query("drop user u{}".format(i))

    for i in range(users):
        node2.query("create user u{}".format(i))

    assert_correct_metric(node2)

    clear(node1, users, 0)
    clear(node2, users, 0)


def test_warnings(started_cluster):
    users = limit - get_metric_value(node)

    for i in range(users):
        node.query("create user u{}".format(i))

    verify_warning_with_values(
        node,
        current_val=users,
        warn_val=warn_limit,
        throw_val=limit
    )

    # Drop decreases the metric
    for i in range(users):
        node.query("drop user u{}".format(i))

    verify_no_warning(node, "The number of access entities")

    clear(node, users, 0)
