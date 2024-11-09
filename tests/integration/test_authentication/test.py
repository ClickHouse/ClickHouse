import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()

        instance.query("CREATE USER sasha")
        instance.query("CREATE USER masha IDENTIFIED BY 'qwerty'")

        yield cluster

    finally:
        cluster.shutdown()


def test_authentication_pass():
    assert instance.query("SELECT currentUser()", user="sasha") == "sasha\n"
    assert (
        instance.query("SELECT currentUser()", user="masha", password="qwerty")
        == "masha\n"
    )

    # 'no_password' authentication type allows to login with any password.
    assert (
        instance.query("SELECT currentUser()", user="sasha", password="something")
        == "sasha\n"
    )
    assert (
        instance.query("SELECT currentUser()", user="sasha", password="something2")
        == "sasha\n"
    )


def test_authentication_fail():
    # User doesn't exist.
    assert "vasya: Authentication failed" in instance.query_and_get_error(
        "SELECT currentUser()", user="vasya"
    )

    # Wrong password.
    assert "masha: Authentication failed" in instance.query_and_get_error(
        "SELECT currentUser()", user="masha", password="123"
    )
