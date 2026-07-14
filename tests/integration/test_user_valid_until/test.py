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
    assert node.query("SELECT valid_until FROM system.users WHERE name = 'user_basic'") == "['1970-01-01 00:00:00']\n"

    # 2. With valid VALID UNTIL
    node.query("ALTER USER user_basic VALID UNTIL '06/11/2040 08:03:20 Z+3'")

    assert (
        node.query("SHOW CREATE USER user_basic")
        == "CREATE USER user_basic IDENTIFIED WITH no_password VALID UNTIL \\'2040-11-06 05:03:20\\'\n"
    )
    assert node.query("SELECT 1", user="user_basic") == "1\n"
    assert node.query("SELECT valid_until FROM system.users WHERE name = 'user_basic'") == "['2040-11-06 05:03:20']\n"
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


def test_valid_for_interval(started_cluster):
    node.query("DROP USER IF EXISTS user_valid_for")

    # VALID FOR is resolved to an absolute deadline and stored (and shown) in the VALID UNTIL form.
    node.query("CREATE USER user_valid_for VALID FOR INTERVAL 50 YEAR")

    show_create = node.query("SHOW CREATE USER user_valid_for")
    assert "VALID UNTIL" in show_create
    assert "VALID FOR" not in show_create
    assert node.query("SELECT 1", user="user_valid_for") == "1\n"

    # A deadline in the past (negative interval) expires the credential immediately.
    node.query("ALTER USER user_valid_for VALID FOR INTERVAL -1 YEAR")

    show_create = node.query("SHOW CREATE USER user_valid_for")
    assert "VALID UNTIL" in show_create
    assert "VALID FOR" not in show_create

    error = "Authentication failed"
    assert error in node.query_and_get_error("SELECT 1", user="user_valid_for")

    node.query("DROP USER IF EXISTS user_valid_for")


def test_valid_for_interval_overflow(started_cluster):
    node.query("DROP USER IF EXISTS user_valid_for_overflow")

    # An absurdly high interval must not overflow: the deadline is computed in DateTime64
    # and saturates at its upper bound (year 2299) instead of wrapping around into the past.
    node.query("CREATE USER user_valid_for_overflow VALID FOR INTERVAL 1000000 YEAR")

    assert node.query("SELECT 1", user="user_valid_for_overflow") == "1\n"
    assert "VALID UNTIL \\'2299-12-31" in node.query(
        "SHOW CREATE USER user_valid_for_overflow"
    )
    # The `valid_until` column of `system.users` is a `DateTime`, which cannot hold year 2299;
    # the value is clamped to the upper bound of `DateTime` instead of wrapping around.
    assert (
        node.query(
            "SELECT valid_until[1] > now() + INTERVAL 50 YEAR, toYear(valid_until[1]) FROM system.users WHERE name = 'user_valid_for_overflow'"
        )
        == "1\t2106\n"
    )

    node.query("DROP USER IF EXISTS user_valid_for_overflow")


def test_valid_for_interval_negative_overflow(started_cluster):
    node.query("DROP USER IF EXISTS user_valid_for_neg_overflow")

    # An absurdly low (negative) interval must not wrap around into the future: the deadline is
    # computed in DateTime64 and saturates at its lower bound (year 1900) instead. The credential
    # is therefore already expired, so login must fail.
    node.query(
        "CREATE USER user_valid_for_neg_overflow VALID FOR INTERVAL -1000000 YEAR"
    )

    error = "Authentication failed"
    assert error in node.query_and_get_error(
        "SELECT 1", user="user_valid_for_neg_overflow"
    )

    # The `valid_until` column of `system.users` is a `DateTime`, which cannot hold the pre-1970
    # (negative) deadline; the value is clamped to 1 (`1970-01-01 00:00:01`) instead of a plain
    # `static_cast<UInt32>` wrapping the negative `time_t` into a far-future timestamp. It stays
    # distinct from 0, which means "no expiration".
    assert (
        node.query(
            "SELECT toUInt32(valid_until[1]) FROM system.users WHERE name = 'user_valid_for_neg_overflow'"
        )
        == "1\n"
    )

    node.query("DROP USER IF EXISTS user_valid_for_neg_overflow")


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
