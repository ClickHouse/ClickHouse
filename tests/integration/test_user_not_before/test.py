from datetime import datetime, timedelta
from time import sleep

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_basic(started_cluster):
    node.query("DROP USER IF EXISTS user_basic")

    # 1. Without NOT BEFORE
    node.query("CREATE USER user_basic")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password\n"
    )
    assert node.query("SELECT 1", user="user_basic") == "1\n"

    # 2. With valid NOT BEFORE
    node.query("ALTER USER user_basic NOT BEFORE '06/11/2010 08:03:20 Z+3'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password NOT BEFORE \\'2010-11-06 05:03:20\\'\n"
    )
    assert node.query("SELECT 1", user="user_basic") == "1\n"

    # 3. With expired NOT BEFORE
    node.query("ALTER USER user_basic NOT BEFORE '06/11/2040 08:03:20 Z+3'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password NOT BEFORE \\'2040-11-06 05:03:20\\'\n"
    )

    error = "Authentication failed"
    assert error in node.query_and_get_error("SELECT 1", user="user_basic")

    # 4. Reset NOT BEFORE
    node.query("ALTER USER user_basic NOT BEFORE 'infinity'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password\n"
    )
    assert node.query("SELECT 1", user="user_basic") == "1\n"
    node.query("DROP USER user_basic")

    # 5. Make NOT BEFORE expire
    before_datetime = datetime.today() + timedelta(0, 10)
    before_string = before_datetime.strftime("%Y-%m-%d %H:%M:%S")

    node.query(f"CREATE USER user_basic NOT BEFORE '{before_string}'")

    error = "Authentication failed"
    assert error in node.query_and_get_error("SELECT 1", user="user_basic")

    sleep(12)

    assert node.query("SELECT 1", user="user_basic") == "1\n"

    node.query("DROP USER IF EXISTS user_basic")

    # NOT IDENTIFIED test to make sure not before is also parsed on its short-circuit
    node.query("CREATE USER user_basic NOT IDENTIFIED NOT BEFORE '01/01/2040'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password NOT BEFORE \\'2040-01-01 00:00:00\\'\n"
    )

    assert error in node.query_and_get_error("SELECT 1", user="user_basic")

    node.query("DROP USER IF EXISTS user_basic")


def test_details(started_cluster):
    node.query("DROP USER IF EXISTS user_details_infinity, user_details_time_only")

    # 1. Does not do anything
    node.query("CREATE USER user_details_infinity NOT BEFORE 'infinity'")

    assert (
        node.query("SHOW CREATE USER user_details_infinity")
        == "CREATE USER user_details_infinity IDENTIFIED WITH no_password\n"
    )

    # 2. Time only is not supported
    node.query(
        "CREATE USER user_details_time_only IDENTIFIED WITH no_password NOT BEFORE '22:03:40'"
    )

    until_year = datetime.today().strftime("%Y")

    assert (
        node.query("SHOW CREATE USER user_details_time_only")
        == f"CREATE USER user_details_time_only IDENTIFIED WITH no_password NOT BEFORE \\'{until_year}-01-01 22:03:40\\'\n"
    )

    node.query("DROP USER IF EXISTS user_details_infinity, user_details_time_only")


def test_restart(started_cluster):
    node.query("DROP USER IF EXISTS user_restart")

    node.query("CREATE USER user_restart NOT BEFORE '06/11/2040 08:03:20 Z+3'")

    assert (
        node.query("SHOW CREATE USER user_restart")
        == "CREATE USER user_restart IDENTIFIED WITH no_password NOT BEFORE \\'2040-11-06 05:03:20\\'\n"
    )

    node.restart_clickhouse()

    assert (
        node.query("SHOW CREATE USER user_restart")
        == "CREATE USER user_restart IDENTIFIED WITH no_password NOT BEFORE \\'2040-11-06 05:03:20\\'\n"
    )

    error = "Authentication failed"
    assert error in node.query_and_get_error("SELECT 1", user="user_restart")

    node.query("DROP USER IF EXISTS user_restart")


def test_multiple_authentication_methods(started_cluster):
    node.query("DROP USER IF EXISTS user_basic")

    node.query(
        "CREATE USER user_basic IDENTIFIED WITH plaintext_password BY 'no_expiration',"
        "plaintext_password by 'not_expired' NOT BEFORE '06/11/2010', plaintext_password by 'expired' NOT BEFORE '06/11/2040',"
        "plaintext_password by 'infinity' NOT BEFORE 'infinity'"
    )

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH plaintext_password, plaintext_password NOT BEFORE \\'2010-11-06 00:00:00\\', "
        "plaintext_password NOT BEFORE \\'2040-11-06 00:00:00\\', plaintext_password\n"
    )
    assert node.query("SELECT 1", user="user_basic", password="no_expiration") == "1\n"
    assert node.query("SELECT 1", user="user_basic", password="not_expired") == "1\n"
    assert node.query("SELECT 1", user="user_basic", password="infinity") == "1\n"

    error = "Authentication failed"
    assert error in node.query_and_get_error(
        "SELECT 1", user="user_basic", password="expired"
    )

    # Expire them all
    node.query("ALTER USER user_basic NOT BEFORE '06/11/2040 08:03:20'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH plaintext_password NOT BEFORE \\'2040-11-06 08:03:20\\',"
        " plaintext_password NOT BEFORE \\'2040-11-06 08:03:20\\',"
        " plaintext_password NOT BEFORE \\'2040-11-06 08:03:20\\',"
        " plaintext_password NOT BEFORE \\'2040-11-06 08:03:20\\'\n"
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
