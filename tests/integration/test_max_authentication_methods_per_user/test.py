import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)

limited_node = cluster.add_instance(
    "limited_node",
    main_configs=["configs/max_auth_limited.xml"],
)

default_node = cluster.add_instance(
    "default_node",
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_create(started_cluster):
    expected_error = "User can not be created/updated because it exceeds the allowed quantity of authentication methods per user"
    assert expected_error in limited_node.query_and_get_error("CREATE USER u_max_authentication_methods IDENTIFIED BY '1', BY '2', BY '3'")


    assert expected_error not in limited_node.query_and_get_answer_with_error("CREATE USER u_max_authentication_methods IDENTIFIED BY '1', BY '2'")

    limited_node.query("DROP USER u_max_authentication_methods")

def test_alter(started_cluster):
    limited_node.query("CREATE USER u_max_authentication_methods IDENTIFIED BY '1'")

    expected_error = "User can not be created/updated because it exceeds the allowed quantity of authentication methods per user"
    assert expected_error in limited_node.query_and_get_error("ALTER USER u_max_authentication_methods ADD IDENTIFIED BY '2', BY '3'")

    expected_error = "User can not be created/updated because it exceeds the allowed quantity of authentication methods per user"
    assert expected_error in limited_node.query_and_get_error("ALTER USER u_max_authentication_methods IDENTIFIED BY '3', BY '4', BY '5'")

    assert expected_error not in limited_node.query_and_get_answer_with_error("ALTER USER u_max_authentication_methods ADD IDENTIFIED BY '2'")

    assert expected_error not in limited_node.query_and_get_answer_with_error("ALTER USER u_max_authentication_methods IDENTIFIED BY '2', BY '3'")

    limited_node.query("DROP USER u_max_authentication_methods")
