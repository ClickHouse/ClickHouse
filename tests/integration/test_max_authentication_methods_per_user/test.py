import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

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


expected_error = "User can not be created/updated because it exceeds the allowed quantity of authentication methods per user"


def test_create(started_cluster):

    assert expected_error in limited_node.query_and_get_error(
        "CREATE USER u_max_authentication_methods IDENTIFIED BY '1', BY '2', BY '3'"
    )

    assert expected_error not in limited_node.query_and_get_answer_with_error(
        "CREATE USER u_max_authentication_methods IDENTIFIED BY '1', BY '2'"
    )

    limited_node.query("DROP USER u_max_authentication_methods")


def test_alter(started_cluster):
    limited_node.query("CREATE USER u_max_authentication_methods IDENTIFIED BY '1'")

    assert expected_error in limited_node.query_and_get_error(
        "ALTER USER u_max_authentication_methods ADD IDENTIFIED BY '2', BY '3'"
    )

    assert expected_error in limited_node.query_and_get_error(
        "ALTER USER u_max_authentication_methods IDENTIFIED BY '3', BY '4', BY '5'"
    )

    assert expected_error not in limited_node.query_and_get_answer_with_error(
        "ALTER USER u_max_authentication_methods ADD IDENTIFIED BY '2'"
    )

    assert expected_error not in limited_node.query_and_get_answer_with_error(
        "ALTER USER u_max_authentication_methods IDENTIFIED BY '2', BY '3'"
    )

    limited_node.query("DROP USER u_max_authentication_methods")


def get_query_with_multiple_identified_with(
    operation, username, identified_with_count, add_operation=""
):
    identified_clauses = ", ".join([f"BY '1'" for _ in range(identified_with_count)])
    query = (
        f"{operation} USER {username} {add_operation} IDENTIFIED {identified_clauses}"
    )
    return query


def test_create_default_setting(started_cluster):
    expected_error = "User can not be created/updated because it exceeds the allowed quantity of authentication methods per user"

    query_exceeds = get_query_with_multiple_identified_with(
        "CREATE", "u_max_authentication_methods", 101
    )

    assert expected_error in default_node.query_and_get_error(query_exceeds)

    query_not_exceeds = get_query_with_multiple_identified_with(
        "CREATE", "u_max_authentication_methods", 100
    )

    assert expected_error not in default_node.query_and_get_answer_with_error(
        query_not_exceeds
    )

    default_node.query("DROP USER u_max_authentication_methods")


def test_alter_default_setting(started_cluster):
    default_node.query("CREATE USER u_max_authentication_methods IDENTIFIED BY '1'")

    query_add_exceeds = get_query_with_multiple_identified_with(
        "ALTER", "u_max_authentication_methods", 100, "ADD"
    )

    assert expected_error in default_node.query_and_get_error(query_add_exceeds)

    query_replace_exceeds = get_query_with_multiple_identified_with(
        "ALTER", "u_max_authentication_methods", 101
    )

    assert expected_error in default_node.query_and_get_error(query_replace_exceeds)

    query_add_not_exceeds = get_query_with_multiple_identified_with(
        "ALTER", "u_max_authentication_methods", 99, "ADD"
    )

    assert expected_error not in default_node.query_and_get_answer_with_error(
        query_add_not_exceeds
    )

    query_replace_not_exceeds = get_query_with_multiple_identified_with(
        "ALTER", "u_max_authentication_methods", 100
    )

    assert expected_error not in default_node.query_and_get_answer_with_error(
        query_replace_not_exceeds
    )

    default_node.query("DROP USER u_max_authentication_methods")
