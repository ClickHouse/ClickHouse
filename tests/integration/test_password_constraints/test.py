import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["configs/complexity_rules.xml"])
node1 = cluster.add_instance("node1", main_configs=["configs/allow_implicit_no_password.xml"])

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_complexity_rules(start_cluster):

    error_message = "DB::Exception: Invalid password. The password should: be at least 12 characters long, contain at least 1 numeric character, contain at least 1 lowercase character, contain at least 1 uppercase character, contain at least 1 special character"
    assert error_message in node.query_and_get_error(
        "CREATE USER u_1 IDENTIFIED WITH plaintext_password BY ''"
    )

    error_message = "DB::Exception: Invalid password. The password should: contain at least 1 lowercase character, contain at least 1 uppercase character, contain at least 1 special character"
    assert error_message in node.query_and_get_error(
        "CREATE USER u_2 IDENTIFIED WITH sha256_password BY '000000000000'"
    )

    error_message = "DB::Exception: Invalid password. The password should: contain at least 1 uppercase character, contain at least 1 special character"
    assert error_message in node.query_and_get_error(
        "CREATE USER u_3 IDENTIFIED WITH double_sha1_password BY 'a00000000000'"
    )

    error_message = "DB::Exception: Invalid password. The password should: contain at least 1 special character"
    assert error_message in node.query_and_get_error(
        "CREATE USER u_4 IDENTIFIED WITH plaintext_password BY 'aA0000000000'"
    )

    node.query("CREATE USER u_5 IDENTIFIED WITH plaintext_password BY 'aA!000000000'")
    node.query("DROP USER u_5")


def test_allow_implicit_no_password():

    error_message = "Authentication type NO_PASSWORD must be explicitly specified, check the setting allow_implicit_no_password in the server configuration"
    assert error_message in node1.query_and_get_error(
        "CREATE USER u1"
    )

    node1.query("CREATE USER u2 IDENTIFIED WITH no_password")
    node1.query("CREATE USER u3 IDENTIFIED BY 'qwe123'")
    node1.query("DROP USER u2, u3")