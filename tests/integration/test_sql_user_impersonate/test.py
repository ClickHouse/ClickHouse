import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node"
)

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_sql_impersonate():

    node.query(
        "CREATE USER user1 IDENTIFIED WITH plaintext_password BY 'password1';"
        "CREATE USER user2 IDENTIFIED WITH plaintext_password BY 'password2';"
    )

    queries = [
        "EXECUTE AS user2 SELECT * from system.tables;",
        "EXECUTE AS default SELECT * from system.tables;",
        "EXECUTE AS default",
        "GRANT IMPERSONATE ON default TO user2; EXECUTE AS user2;",
        "GRANT IMPERSONATE ON default TO user2; EXECUTE AS default;",
        "GRANT IMPERSONATE ON user2 TO user1; EXECUTE AS user1;"
    ]

    errors = []
    for q in queries:
        err = node.query_and_get_error(q)
        errors.append((q, err))

    node.query("DROP USER IF EXISTS user1;")
    node.query("DROP USER IF EXISTS user2;")

    for q, err in errors:
        assert "IMPERSONATE feature is disabled" in err, f"Unexpected error for query:\n{q}\nError: {err}"
