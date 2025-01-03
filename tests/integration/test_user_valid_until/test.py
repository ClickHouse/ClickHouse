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

    # 1. Without VALID UNTIL
    node.query("CREATE USER user_basic")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password\n"
    )
    assert node.query("SELECT 1", user="user_basic") == "1\n"

    # 2. With valid VALID UNTIL
    node.query("ALTER USER user_basic VALID UNTIL '06/11/2040 08:03:20 Z+3'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password VALID UNTIL \\'2040-11-06 05:03:20\\'\n"
    )
    assert node.query("SELECT 1", user="user_basic") == "1\n"

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
