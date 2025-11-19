import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    stay_alive=True,
)

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_sql_impersonate():

    node.get_query_request(
        "CREATE  USER user1 IDENTIFIED WITH plaintext_password BY 'password1';"
        "CREATE  USER user2 IDENTIFIED WITH plaintext_password BY 'password2';"
    )

    errors = []

    errors.append(node.get_query_request(
        "EXECUTE AS user2 SELECT * from system.tables;"
    ).get_error())

    errors.append(node.get_query_request(
        "EXECUTE AS default SELECT * from system.tables;"
    ).get_error())

    errors.append(node.get_query_request(
        "EXECUTE AS default"
    ).get_error())

    errors.append(node.get_query_request(
        "GRANT IMPERSONATE ON user1 TO user2;"
    ).get_error())

    errors.append(node.get_query_request(
        "GRANT IMPERSONATE ON * TO user2;"
    ).get_error())

    assert len(errors) == 5
    assert all("IMPERSONATE feature is disabled" in x for x in errors)

