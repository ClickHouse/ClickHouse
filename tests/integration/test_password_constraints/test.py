import pytest
import os

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)

n1 = cluster.add_instance("n1", main_configs=["configs/complexity_rules.xml"])
n2 = cluster.add_instance(
    "n2", main_configs=["configs/allow_implicit_no_password.xml"])
n3 = cluster.add_instance("n3", stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_complexity_rules():

    error_message = "DB::Exception: Invalid password. The password should: be at least 12 characters long, contain at least 1 numeric character, contain at least 1 lowercase character, contain at least 1 uppercase character, contain at least 1 special character"
    assert error_message in n1.query_and_get_error(
        "CREATE USER u_1 IDENTIFIED WITH plaintext_password BY ''"
    )

    error_message = "DB::Exception: Invalid password. The password should: contain at least 1 lowercase character, contain at least 1 uppercase character, contain at least 1 special character"
    assert error_message in n1.query_and_get_error(
        "CREATE USER u_2 IDENTIFIED WITH sha256_password BY '000000000000'"
    )

    error_message = "DB::Exception: Invalid password. The password should: contain at least 1 uppercase character, contain at least 1 special character"
    assert error_message in n1.query_and_get_error(
        "CREATE USER u_3 IDENTIFIED WITH double_sha1_password BY 'a00000000000'"
    )

    error_message = "DB::Exception: Invalid password. The password should: contain at least 1 special character"
    assert error_message in n1.query_and_get_error(
        "CREATE USER u_4 IDENTIFIED WITH plaintext_password BY 'aA0000000000'"
    )

    n1.query("CREATE USER u_5 IDENTIFIED WITH plaintext_password BY 'aA!000000000'")
    n1.query("DROP USER u_5")


def test_allow_implicit_no_password():

    error_message = "Authentication type NO_PASSWORD must be explicitly specified, check the setting allow_implicit_no_password in the server configuration"
    assert error_message in n2.query_and_get_error(
        "CREATE USER u1"
    )

    n2.query("CREATE USER u2 IDENTIFIED WITH no_password")
    n2.query("CREATE USER u3 IDENTIFIED BY 'qwe123'")
    n2.query("DROP USER u2, u3")


def test_allow_plaintext_and_no_password():
    n3.stop_clickhouse()

    n3.exec_in_container(
        [
            "bash",
            "-c",
            "find /etc/clickhouse-server/ -type f -name '*users.xml' -delete"
        ]
    )

    n3.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/users.xml"),
        "/etc/clickhouse-server/users.xml",
    )

    n3.start_clickhouse(password='pwd')

    n3.query("CREATE USER u IDENTIFIED WITH double_sha1_hash BY '8DCDD69CE7D121DE8013062AEAEB2A148910D50E'", password="pwd")
    n3.query("CREATE USER u1 IDENTIFIED BY 'qwe123'", password="pwd")

    error_message = "is not allowed, check the setting allow_"
    assert error_message in n3.query_and_get_error(
        "CREATE USER u2_02207 HOST IP '127.1' IDENTIFIED WITH plaintext_password BY 'qwerty'", password="pwd"
    )

    assert error_message in n3.query_and_get_error(
        "CREATE USER u3_02207 HOST IP '127.1' IDENTIFIED WITH no_password", password="pwd"
    )

    assert error_message in n3.query_and_get_error(
        "CREATE USER u4_02207 HOST IP '127.1' NOT IDENTIFIED", password="pwd"
    )

    assert error_message in n3.query_and_get_error(
        "CREATE USER IF NOT EXISTS u5_02207", password="pwd"
    )

    n3.query("DROP USER u, u1", password="pwd")
