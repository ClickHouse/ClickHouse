import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/functions_requiring_grant.xml"],
    user_configs=["configs/users.d/users.xml"],
    stay_alive=True,
)

EXECUTABLE_FUNCTIONS_CONFIG = """<clickhouse>
    <user_defined_executable_functions_config>/etc/clickhouse-server/functions/listed_executable_function.xml</user_defined_executable_functions_config>
</clickhouse>"""


def copy_dir_to_container(local_path, dist_path, container_id):
    os.system(
        f"docker cp {os.path.join(SCRIPT_DIR, local_path)}/. {container_id}:{dist_path}"
    )


def skip_test_msan():
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with vfork")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()

        copy_dir_to_container(
            "functions", "/etc/clickhouse-server/functions", instance.docker_id
        )
        copy_dir_to_container(
            "user_scripts", "/var/lib/clickhouse/user_scripts", instance.docker_id
        )
        instance.replace_config(
            "/etc/clickhouse-server/config.d/executable_functions.xml",
            EXECUTABLE_FUNCTIONS_CONFIG,
        )
        instance.restart_clickhouse()

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instance.query("DROP USER IF EXISTS A, B")
        instance.query("DROP TABLE IF EXISTS t")
        instance.query("DROP FUNCTION IF EXISTS wrap_hex")
        instance.query("DROP FUNCTION IF EXISTS listed_udf")


def test_unlisted_function_does_not_need_grant():
    instance.query("CREATE USER A")
    assert instance.query("SELECT plus(1, 2)", user="A") == "3\n"


def test_listed_function_requires_grant():
    instance.query("CREATE USER A")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT hex('a')", user="A"
    )

    instance.query("GRANT FUNCTION ON hex TO A")
    assert instance.query("SELECT hex('a')", user="A") == "61\n"

    instance.query("REVOKE FUNCTION ON hex FROM A")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT hex('a')", user="A"
    )


def test_grant_function_on_star():
    instance.query("CREATE USER A")
    instance.query("GRANT FUNCTION ON * TO A")
    assert instance.query("SELECT hex('a')", user="A") == "61\n"


def test_grant_all_includes_function():
    instance.query("CREATE USER A")
    instance.query("GRANT ALL ON *.* TO A")
    assert instance.query("SELECT hex('a')", user="A") == "61\n"


def test_grant_for_another_function_is_not_enough():
    instance.query("CREATE USER A")
    instance.query("GRANT FUNCTION ON plus TO A")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT hex('a')", user="A"
    )


def test_udf_body_is_checked():
    instance.query("CREATE USER A")
    instance.query("CREATE FUNCTION wrap_hex AS (x) -> hex(x)")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT wrap_hex('a')", user="A"
    )

    instance.query("GRANT FUNCTION ON hex TO A")
    assert instance.query("SELECT wrap_hex('a')", user="A") == "61\n"


def test_listed_sql_udf_requires_grant():
    instance.query("CREATE USER A")
    instance.query("CREATE FUNCTION listed_udf AS (x) -> plus(x, 1)")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT listed_udf(1)", user="A"
    )
    assert instance.query("SELECT plus(1, 1)", user="A") == "2\n"

    instance.query("GRANT FUNCTION ON listed_udf TO A")
    assert instance.query("SELECT listed_udf(1)", user="A") == "2\n"

    instance.query("REVOKE FUNCTION ON listed_udf FROM A")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT listed_udf(1)", user="A"
    )


def test_lambda_alias_shadowing_listed_function():
    """A lambda alias and a lambda parameter have priority over the function `hex`. The analyzer
    only looks at it to find out what the name means, so no grant is needed."""
    instance.query("CREATE USER A")

    assert (
        instance.query("WITH x -> x + 1 AS hex SELECT arrayMap(hex, [1])", user="A")
        == "[2]\n"
    )
    assert (
        instance.query(
            "WITH hex > 0 AS p SELECT arrayFilter(hex -> p, [1, 2])", user="A"
        )
        == "[1,2]\n"
    )


def test_function_without_parentheses_requires_grant():
    """`currentUser` may be called without parentheses, which the analyzer resolves on a separate path."""
    instance.query("CREATE USER A")

    for query in [
        "SELECT currentUser()",
        "SELECT currentUser",
        "SELECT current_user",
        "WITH currentUser AS u SELECT u",
    ]:
        assert "Not enough privileges" in instance.query_and_get_error(
            query, user="A"
        ), query

    # A column with the same name takes priority, so the function is not called.
    assert (
        instance.query("SELECT currentUser FROM (SELECT 1 AS currentUser)", user="A")
        == "1\n"
    )

    instance.query("GRANT FUNCTION ON currentUser TO A")
    assert instance.query("SELECT currentUser", user="A") == "A\n"
    assert instance.query("SELECT current_user", user="A") == "A\n"


def test_values_expression_requires_grant():
    """Expressions in `VALUES` are evaluated by the legacy `ExpressionAnalyzer`, not by the
    analyzer. Sent over HTTP, because `clickhouse-client` would evaluate them itself, and without
    `async_insert`, which evaluates them later."""
    instance.query("CREATE TABLE t (s String) ENGINE = Memory")
    instance.query("CREATE USER A")
    instance.query("GRANT INSERT ON t TO A")

    query = "INSERT INTO t SETTINGS async_insert = 0 VALUES (hex('a'))"
    assert "Not enough privileges" in instance.http_query_and_get_error(
        query, method="POST", user="A"
    )

    instance.query("GRANT FUNCTION ON hex TO A")
    instance.http_query(query, method="POST", user="A")
    assert instance.query("SELECT s FROM t") == "61\n"


def test_function_selected_by_setting_requires_grant():
    """The grant is required for the function which is created for the query: under
    `use_legacy_to_time` it is `toTimeWithFixedDate` for a call of `toTime`."""
    instance.query("CREATE USER A")

    query = (
        "SELECT toTime(toDateTime('2026-01-01 12:00:00')) "
        "SETTINGS use_legacy_to_time = 1"
    )
    assert "Not enough privileges" in instance.query_and_get_error(query, user="A")

    instance.query("GRANT FUNCTION ON toTimeWithFixedDate TO A")
    assert instance.query(query, user="A") == instance.query(query)


def test_system_functions_readable_without_grant():
    """Listing functions is not calling them, so it shows the listed ones in full to a user who
    cannot execute them."""
    instance.query("CREATE USER A")
    instance.query("GRANT SELECT ON system.functions TO A")
    instance.query("CREATE FUNCTION listed_udf AS (x) -> plus(x, 1)")

    assert (
        instance.query(
            "SELECT count() FROM system.functions WHERE name = 'listed_udf'", user="A"
        )
        == "1\n"
    )
    assert (
        instance.query(
            "SELECT deterministic FROM system.functions WHERE name = 'hex'", user="A"
        )
        == "1\n"
    )
    assert "listed_udf" in instance.query("SHOW FUNCTIONS ILIKE 'listed_udf'", user="A")


def test_higher_order_function_argument_requires_grant():
    """`arrayMap(hex, ...)` is rewritten into a lambda that calls `hex`, so the grant applies."""
    instance.query("CREATE USER A")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT arrayMap(hex, ['a'])", user="A"
    )

    instance.query("GRANT FUNCTION ON hex TO A")
    assert instance.query("SELECT arrayMap(hex, ['a'])", user="A") == "['61']\n"


def test_case_insensitive_spelling_requires_grant():
    instance.query("CREATE USER A")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT HEX('a')", user="A"
    )

    instance.query("GRANT FUNCTION ON hex TO A")
    assert instance.query("SELECT HEX('a')", user="A") == "61\n"


def test_case_sensitive_alias_requires_grant():
    """`isASCII` is a case-sensitive alias of the listed `isValidASCII`, and those are not covered
    by the case-insensitive name mapping, so they used to slip past the check."""
    instance.query("CREATE USER A")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT isASCII('a')", user="A"
    )

    instance.query("GRANT FUNCTION ON isValidASCII TO A")
    assert instance.query("SELECT isASCII('a')", user="A") == "1\n"


def test_alias_of_case_insensitive_function_requires_grant():
    """`fullHostName` is a case-sensitive alias of the case-insensitive `FQDN`."""
    instance.query("CREATE USER A")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT fullHostName()", user="A"
    )

    instance.query("GRANT FUNCTION ON FQDN TO A")
    assert instance.query("SELECT fullHostName() != ''", user="A") == "1\n"


def test_alias_listed_in_other_case_requires_grant():
    """`LCASE` in the config is a case-insensitive alias of `lower` written in another case,
    so it must protect `lower` and every spelling of the alias."""
    instance.query("CREATE USER A")

    for query in ["SELECT lower('A')", "SELECT lcase('A')", "SELECT LCASE('A')"]:
        assert "Not enough privileges" in instance.query_and_get_error(query, user="A")

    instance.query("GRANT FUNCTION ON lower TO A")
    assert instance.query("SELECT lower('A')", user="A") == "a\n"
    assert instance.query("SELECT LCASE('A')", user="A") == "a\n"


def test_grant_by_other_spelling_or_alias():
    """The name in GRANT and REVOKE is resolved like the checked call, so any spelling works."""
    instance.query("CREATE USER A")

    instance.query("GRANT FUNCTION ON HEX TO A")
    assert instance.query("SELECT hex('a')", user="A") == "61\n"
    assert instance.query("SHOW GRANTS FOR A") == TSV(["GRANT FUNCTION ON hex TO A"])
    assert instance.query("CHECK GRANT FUNCTION ON HEX", user="A") == "1\n"

    instance.query("REVOKE FUNCTION ON HEX FROM A")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT hex('a')", user="A"
    )

    instance.query("GRANT FUNCTION ON isASCII TO A")
    assert instance.query("SELECT isValidASCII('a')", user="A") == "1\n"
    assert instance.query("SHOW GRANTS FOR A") == TSV(
        ["GRANT FUNCTION ON isValidASCII TO A"]
    )


def test_wildcard_grant_is_not_resolved():
    """A wildcard is a prefix of registered names: resolving `LCASE*` to `lower*` would grant
    other functions."""
    instance.query("CREATE USER A")

    instance.query("GRANT FUNCTION ON LCASE* TO A")
    assert instance.query("SHOW GRANTS FOR A") == TSV(["GRANT FUNCTION ON LCASE* TO A"])
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT lower('A')", user="A"
    )

    instance.query("REVOKE FUNCTION ON LCASE* FROM A")
    instance.query("GRANT FUNCTION ON he* TO A")
    assert instance.query("SELECT hex('a')", user="A") == "61\n"


def test_authentication_grants_by_other_spelling_or_alias():
    """The GRANTS clause of an authentication method limits the rights of the user by intersection,
    so a function name in it must be resolved like in GRANT."""
    instance.query(
        "CREATE USER A IDENTIFIED WITH sha256_password BY 'p' "
        "GRANTS (FUNCTION ON HEX, FUNCTION ON isASCII)"
    )
    instance.query("GRANT FUNCTION ON HEX, FUNCTION ON isASCII TO A")

    assert instance.query("SELECT hex('a')", user="A", password="p") == "61\n"
    assert instance.query("SELECT isValidASCII('a')", user="A", password="p") == "1\n"


def test_listed_executable_udf_requires_grant():
    skip_test_msan()
    instance.query("CREATE USER A")

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT listed_executable_udf(toUInt64(1))", user="A"
    )

    instance.query("GRANT FUNCTION ON listed_executable_udf TO A")
    assert (
        instance.query("SELECT listed_executable_udf(toUInt64(1))", user="A")
        == "Key 1\n"
    )
