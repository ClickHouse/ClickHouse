import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instance.query("DROP USER IF EXISTS A, B")


def test_login():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    assert instance.query("SELECT 1", user='A') == "1\n"
    assert instance.query("SELECT 1", user='B') == "1\n"


def test_grant_create_user():
    instance.query("CREATE USER A")

    expected_error = "Not enough privileges"
    assert expected_error in instance.query_and_get_error("CREATE USER B", user='A')

    instance.query("GRANT CREATE USER ON *.* TO A")
    instance.query("CREATE USER B", user='A')
    assert instance.query("SELECT 1", user='B') == "1\n"
