from datetime import datetime, timedelta
from time import sleep

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", user_configs=["configs/users.xml"], stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_basic(started_cluster):
    node.query("DROP USER IF EXISTS user_basic")

    # 1. Without VALID UNTIL
    node.query("CREATE USER user_basic")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password\n"
    )
    assert node.query("SELECT 1", user="user_basic") == "1\n"
    assert (
        node.query("SELECT valid_until FROM system.users WHERE name = 'user_basic'")
        == "['1970-01-01 00:00:00']\n"
    )

    # 2. With valid VALID UNTIL
    node.query("ALTER USER user_basic VALID UNTIL '06/11/2040 08:03:20 Z+3'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password VALID UNTIL \\'2040-11-06 05:03:20\\'\n"
    )
    assert node.query("SELECT 1", user="user_basic") == "1\n"
    assert (
        node.query("SELECT valid_until FROM system.users WHERE name = 'user_basic'")
        == "['2040-11-06 05:03:20']\n"
    )
    # 3. With expired VALID UNTIL
    node.query("ALTER USER user_basic VALID UNTIL '06/11/2010 08:03:20 Z+3'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password VALID UNTIL \\'2010-11-06 05:03:20\\'\n"
    )

    error = "Authentication failed"
    assert error in node.query_and_get_error("SELECT 1", user="user_basic")

    # 4. Reset VALID UNTIL
    node.query("ALTER USER user_basic VALID UNTIL 'infinity'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password\n"
    )
    assert node.query("SELECT 1", user="user_basic") == "1\n"
    node.query("DROP USER user_basic")

    # 5. Make VALID UNTIL expire
    until_datetime = datetime.today() + timedelta(0, 10)
    until_string = until_datetime.strftime("%Y-%m-%d %H:%M:%S")

    node.query(f"CREATE USER user_basic VALID UNTIL '{until_string}'")

    assert node.query("SELECT 1", user="user_basic") == "1\n"

    sleep(12)

    error = "Authentication failed"
    assert error in node.query_and_get_error("SELECT 1", user="user_basic")

    node.query("DROP USER IF EXISTS user_basic")

    # NOT IDENTIFIED test to make sure valid until is also parsed on its short-circuit
    node.query("CREATE USER user_basic NOT IDENTIFIED VALID UNTIL '01/01/2010'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password VALID UNTIL \\'2010-01-01 00:00:00\\'\n"
    )

    assert error in node.query_and_get_error("SELECT 1", user="user_basic")

    node.query("DROP USER IF EXISTS user_basic")

    # 6. XML form, flat, future expiration: login allowed
    assert (
        node.query("SELECT currentUser()", user="xml_flat_valid", password="x")
        == "xml_flat_valid\n"
    )
    assert (
        node.query("SELECT valid_until FROM system.users WHERE name = 'xml_flat_valid'")
        == "['2099-01-01 00:00:00']\n"
    )

    # 7. XML form, flat, already expired: login rejected
    assert error in node.query_and_get_error(
        "SELECT 1", user="xml_flat_expired", password="x"
    )
    assert (
        node.query(
            "SELECT valid_until FROM system.users WHERE name = 'xml_flat_expired'"
        )
        == "['2010-01-01 00:00:00']\n"
    )

    # 8. XML form with non-plaintext auth (sha256_hex of 'secret') + valid_until
    assert (
        node.query("SELECT currentUser()", user="xml_sha256_valid", password="secret")
        == "xml_sha256_valid\n"
    )
    assert (
        node.query(
            "SELECT valid_until FROM system.users WHERE name = 'xml_sha256_valid'"
        )
        == "['2099-01-01 00:00:00']\n"
    )

    # 9. ALTER on a config-defined user (read-only users_xml storage) must fail
    assert node.query_and_get_error(
        "ALTER USER xml_flat_valid VALID UNTIL '2050-01-01'"
    )


def test_details(started_cluster):
    node.query("DROP USER IF EXISTS user_details_infinity, user_details_time_only")

    # 1. Does not do anything
    node.query("CREATE USER user_details_infinity VALID UNTIL 'infinity'")

    assert (
        node.query("SHOW CREATE USER user_details_infinity")
        == "CREATE USER user_details_infinity IDENTIFIED WITH no_password\n"
    )

    # 2. Time only is not supported
    node.query(
        "CREATE USER user_details_time_only IDENTIFIED WITH no_password VALID UNTIL '22:03:40'"
    )

    until_year = datetime.today().strftime("%Y")

    assert (
        node.query("SHOW CREATE USER user_details_time_only")
        == f"CREATE USER user_details_time_only IDENTIFIED WITH no_password VALID UNTIL \\'{until_year}-01-01 22:03:40\\'\n"
    )

    node.query("DROP USER IF EXISTS user_details_infinity, user_details_time_only")

    # 3. XML form, 'infinity' literal -> time_t = 0 -> '1970-01-01 00:00:00'
    assert (
        node.query("SELECT currentUser()", user="xml_infinity", password="x")
        == "xml_infinity\n"
    )
    assert (
        node.query("SELECT valid_until FROM system.users WHERE name = 'xml_infinity'")
        == "['1970-01-01 00:00:00']\n"
    )


def test_restart(started_cluster):
    node.query("DROP USER IF EXISTS user_restart")

    node.query("CREATE USER user_restart VALID UNTIL '06/11/2010 08:03:20 Z+3'")

    assert (
        node.query("SHOW CREATE USER user_restart")
        == "CREATE USER user_restart IDENTIFIED WITH no_password VALID UNTIL \\'2010-11-06 05:03:20\\'\n"
    )

    node.restart_clickhouse()

    assert (
        node.query("SHOW CREATE USER user_restart")
        == "CREATE USER user_restart IDENTIFIED WITH no_password VALID UNTIL \\'2010-11-06 05:03:20\\'\n"
    )

    error = "Authentication failed"
    assert error in node.query_and_get_error("SELECT 1", user="user_restart")

    node.query("DROP USER IF EXISTS user_restart")


def test_multiple_authentication_methods(started_cluster):
    node.query("DROP USER IF EXISTS user_basic")

    node.query(
        "CREATE USER user_basic IDENTIFIED WITH plaintext_password BY 'no_expiration',"
        "plaintext_password by 'not_expired' VALID UNTIL '06/11/2040', plaintext_password by 'expired' VALID UNTIL '06/11/2010',"
        "plaintext_password by 'infinity' VALID UNTIL 'infinity'"
    )

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH plaintext_password, plaintext_password VALID UNTIL \\'2040-11-06 00:00:00\\', "
        "plaintext_password VALID UNTIL \\'2010-11-06 00:00:00\\', plaintext_password\n"
    )
    assert node.query("SELECT 1", user="user_basic", password="no_expiration") == "1\n"
    assert node.query("SELECT 1", user="user_basic", password="not_expired") == "1\n"
    assert node.query("SELECT 1", user="user_basic", password="infinity") == "1\n"

    error = "Authentication failed"
    assert error in node.query_and_get_error(
        "SELECT 1", user="user_basic", password="expired"
    )

    # Expire them all
    node.query("ALTER USER user_basic VALID UNTIL '06/11/2010 08:03:20'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH plaintext_password VALID UNTIL \\'2010-11-06 08:03:20\\',"
        " plaintext_password VALID UNTIL \\'2010-11-06 08:03:20\\',"
        " plaintext_password VALID UNTIL \\'2010-11-06 08:03:20\\',"
        " plaintext_password VALID UNTIL \\'2010-11-06 08:03:20\\'\n"
    )

    assert error in node.query_and_get_error(
        "SELECT 1", user="user_basic", password="no_expiration"
    )
    assert error in node.query_and_get_error(
        "SELECT 1", user="user_basic", password="not_expired"
    )
    assert error in node.query_and_get_error(
        "SELECT 1", user="user_basic", password="infinity"
    )
    assert error in node.query_and_get_error(
        "SELECT 1", user="user_basic", password="expired"
    )

    # XML form with <auth_methods>: per-method valid_until
    assert (
        node.query("SELECT currentUser()", user="xml_per_method", password="good")
        == "xml_per_method\n"
    )
    assert error in node.query_and_get_error(
        "SELECT 1", user="xml_per_method", password="bad"
    )
    assert (
        node.query("SELECT valid_until FROM system.users WHERE name = 'xml_per_method'")
        == "['2099-01-01 00:00:00','2010-01-01 00:00:00']\n"
    )

    # XML form: user-level <valid_until> unconditionally replaces every per-method value
    assert error in node.query_and_get_error(
        "SELECT 1", user="xml_user_level_replaces", password="a"
    )
    assert error in node.query_and_get_error(
        "SELECT 1", user="xml_user_level_replaces", password="b"
    )
    assert (
        node.query(
            "SELECT valid_until FROM system.users WHERE name = 'xml_user_level_replaces'"
        )
        == "['2010-01-01 00:00:00','2010-01-01 00:00:00']\n"
    )
