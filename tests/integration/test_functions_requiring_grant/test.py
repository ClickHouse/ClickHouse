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


def test_show_grants():
    instance.query("CREATE USER A")
    instance.query("GRANT FUNCTION ON hex TO A")
    assert instance.query("SHOW GRANTS FOR A") == TSV(["GRANT FUNCTION ON hex TO A"])

    instance.query("REVOKE FUNCTION ON hex FROM A")
    instance.query("GRANT EXECUTE FUNCTION ON hex TO A")
    assert instance.query("SHOW GRANTS FOR A") == TSV(["GRANT FUNCTION ON hex TO A"])


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
    """The grant check must not run in the non-throwing probes used before name resolution."""
    instance.query("CREATE USER A")

    assert (
        instance.query("WITH x -> x + 1 AS hex SELECT arrayMap(hex, [1])", user="A")
        == "[2]\n"
    )


def test_system_functions_readable_without_grant():
    """A listed SQL UDF must not break introspection for users who cannot execute it."""
    instance.query("CREATE USER A")
    instance.query("GRANT SELECT ON system.functions TO A")
    instance.query("CREATE FUNCTION listed_udf AS (x) -> plus(x, 1)")

    assert (
        instance.query(
            "SELECT count() FROM system.functions WHERE name = 'listed_udf'", user="A"
        )
        == "1\n"
    )
    assert "listed_udf" in instance.query("SHOW FUNCTIONS ILIKE 'listed_udf'", user="A")


def test_listed_function_requires_grant_old_analyzer():
    instance.query("CREATE USER A")
    old_analyzer = {"enable_analyzer": 0}

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT hex('a')", user="A", settings=old_analyzer
    )

    instance.query("GRANT FUNCTION ON hex TO A")
    assert instance.query("SELECT hex('a')", user="A", settings=old_analyzer) == "61\n"


def test_listed_sql_udf_requires_grant_old_analyzer():
    instance.query("CREATE USER A")
    instance.query("CREATE FUNCTION listed_udf AS (x) -> plus(x, 1)")
    old_analyzer = {"enable_analyzer": 0}

    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT listed_udf(1)", user="A", settings=old_analyzer
    )

    instance.query("GRANT FUNCTION ON listed_udf TO A")
    assert instance.query("SELECT listed_udf(1)", user="A", settings=old_analyzer) == "2\n"


def test_decrypt_requires_grant():
    instance.query("CREATE USER A")
    decrypt_query = (
        "SELECT decrypt('aes-256-ecb', encrypt('aes-256-ecb', 'Secret', "
        "'00000000000000000000000000000000'), '00000000000000000000000000000000')"
    )
    assert "Not enough privileges" in instance.query_and_get_error(
        decrypt_query, user="A"
    )

    instance.query("GRANT FUNCTION ON decrypt TO A")
    assert instance.query(decrypt_query, user="A") == "Secret\n"


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


BAD_CONFIGS = [
    pytest.param(
        "<function>sum</function>",
        "Aggregate function 'sum' cannot be listed",
        id="aggregate_function",
    ),
    pytest.param(
        "hex, decrypt",
        "must be listed as <function> elements",
        id="bare_text_list",
    ),
    pytest.param(
        "<functoin>hex</functoin>",
        "Unknown element 'functoin'",
        id="misspelled_element",
    ),
]


@pytest.mark.parametrize("body, expected_error", BAD_CONFIGS)
def test_invalid_config_is_rejected(body, expected_error):
    """Every one of these used to be a silent no-op: the administrator would see a protected
    function in the config while anybody could still call it. The server must refuse to start."""
    config_path = "/etc/clickhouse-server/config.d/functions_requiring_grant.xml"
    original = instance.exec_in_container(["bash", "-c", f"cat {config_path}"])
    bad_config = f"""<clickhouse>
    <access_control_improvements>
        <functions_requiring_grant>{body}</functions_requiring_grant>
    </access_control_improvements>
</clickhouse>
"""

    instance.stop_clickhouse()
    instance.exec_in_container(
        ["bash", "-c", f"cat > {config_path} << 'XMLEOF'\n{bad_config}XMLEOF"]
    )
    try:
        instance.start_clickhouse(expected_to_fail=True)
        assert instance.contains_in_log(expected_error)
    finally:
        instance.exec_in_container(
            ["bash", "-c", f"cat > {config_path} << 'XMLEOF'\n{original}XMLEOF"]
        )
        instance.start_clickhouse()
