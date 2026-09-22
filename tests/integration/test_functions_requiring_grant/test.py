import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/functions_requiring_grant.xml"],
    user_configs=["configs/users.d/users.xml"],
)


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
