import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

initiator = cluster.add_instance("initiator", main_configs=["configs/named_collections.xml"])
remote = cluster.add_instance("remote")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        remote.query("CREATE TABLE remote_only_eval_table (x UInt64) ENGINE = Memory")
        remote.query("INSERT INTO remote_only_eval_table VALUES (42)")
        remote.query("CREATE TABLE remote_only_eval_config (query String) ENGINE = Memory")
        remote.query(
            "INSERT INTO remote_only_eval_config VALUES ('SELECT x FROM remote_only_eval_table')"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_remote_eval_resolves_constant_query_on_remote():
    assert (
        initiator.query(
            """
            SELECT *
            FROM remote('remote', eval('SELECT x FROM remote_only_eval_table'))
            SETTINGS allow_experimental_eval_table_function = 1, enable_analyzer = 1
            """
        )
        == "42\n"
    )


def test_remote_eval_resolves_constant_query_on_remote_with_serialized_plan():
    assert (
        initiator.query(
            """
            SELECT *
            FROM remote('remote', eval('SELECT x FROM remote_only_eval_table'))
            SETTINGS allow_experimental_eval_table_function = 1,
                enable_analyzer = 1,
                serialize_query_plan = 1
            """
        )
        == "42\n"
    )


def test_remote_loop_eval_resolves_constant_query_on_remote():
    assert (
        initiator.query(
            """
            SELECT *
            FROM remote('remote', loop(eval('SELECT x FROM remote_only_eval_table')))
            LIMIT 1
            SETTINGS allow_experimental_eval_table_function = 1, enable_analyzer = 1
            """
        )
        == "42\n"
    )


def test_remote_loop_eval_resolves_constant_query_on_remote_with_serialized_plan():
    assert (
        initiator.query(
            """
            SELECT *
            FROM remote('remote', loop(eval('SELECT x FROM remote_only_eval_table')))
            LIMIT 1
            SETTINGS allow_experimental_eval_table_function = 1,
                enable_analyzer = 1,
                serialize_query_plan = 1
            """
        )
        == "42\n"
    )


def test_remote_view_resolves_query_on_remote_with_serialized_plan():
    assert (
        initiator.query(
            """
            SELECT *
            FROM remote('remote', view(SELECT x FROM remote_only_eval_table))
            SETTINGS enable_analyzer = 1,
                serialize_query_plan = 1
            """
        )
        == "42\n"
    )


def test_remote_view_if_permitted_resolves_query_on_remote():
    assert (
        initiator.query(
            """
            SELECT *
            FROM remote('remote', viewIfPermitted(SELECT x FROM remote_only_eval_table ELSE null('x UInt64')))
            SETTINGS enable_analyzer = 1
            """
        )
        == "42\n"
    )


def test_remote_view_if_permitted_resolves_query_on_remote_with_serialized_plan():
    assert (
        initiator.query(
            """
            SELECT *
            FROM remote('remote', viewIfPermitted(SELECT x FROM remote_only_eval_table ELSE null('x UInt64')))
            SETTINGS enable_analyzer = 1,
                serialize_query_plan = 1
            """
        )
        == "42\n"
    )


def test_remote_eval_insert_select_keeps_input_query_on_remote():
    remote.query("DROP TABLE IF EXISTS remote_eval_insert_result")
    remote.query("CREATE TABLE remote_eval_insert_result (x UInt64) ENGINE = Memory")

    initiator.query(
        """
        INSERT INTO FUNCTION remote('remote', currentDatabase(), remote_eval_insert_result)
        SELECT *
        FROM remote('remote', eval(SELECT query FROM remote_only_eval_config))
        SETTINGS allow_experimental_eval_table_function = 1,
            enable_analyzer = 1,
            parallel_distributed_insert_select = 2
        """
    )

    assert remote.query("SELECT * FROM remote_eval_insert_result") == "42\n"


def test_remote_eval_resolves_with_alias_argument_on_initiator():
    assert (
        initiator.query(
            """
            WITH 'SELECT x FROM remote_only_eval_table' AS q
            SELECT *
            FROM remote('remote', eval(q))
            SETTINGS allow_experimental_eval_table_function = 1, enable_analyzer = 1
            """
        )
        == "42\n"
    )


def test_remote_loop_eval_resolves_with_alias_argument_on_initiator():
    assert (
        initiator.query(
            """
            WITH 'SELECT x FROM remote_only_eval_table' AS q
            SELECT *
            FROM remote('remote', loop(eval(q)))
            LIMIT 1
            SETTINGS allow_experimental_eval_table_function = 1, enable_analyzer = 1
            """
        )
        == "42\n"
    )


def test_remote_loop_eval_resolves_with_alias_argument_on_initiator_with_serialized_plan():
    assert (
        initiator.query(
            """
            WITH 'SELECT x FROM remote_only_eval_table' AS q
            SELECT *
            FROM remote('remote', loop(eval(q)))
            LIMIT 1
            SETTINGS allow_experimental_eval_table_function = 1,
                enable_analyzer = 1,
                serialize_query_plan = 1
            """
        )
        == "42\n"
    )


def test_remote_eval_resolves_concat_alias_arguments_on_initiator():
    assert (
        initiator.query(
            """
            WITH 'SELECT x FROM ' AS a, 'remote_only_eval_table' AS b
            SELECT *
            FROM remote('remote', eval(a || b))
            SETTINGS allow_experimental_eval_table_function = 1, enable_analyzer = 1
            """
        )
        == "42\n"
    )


def test_remote_named_collection_eval_database_override_resolves_alias_on_initiator():
    assert (
        initiator.query(
            """
            WITH 'SELECT x FROM remote_only_eval_table' AS q
            SELECT *
            FROM remote(remote_eval, database = eval(q))
            SETTINGS allow_experimental_eval_table_function = 1, enable_analyzer = 1
            """
        )
        == "42\n"
    )


def test_remote_named_collection_eval_database_override_resolves_alias_on_initiator_with_serialized_plan():
    assert (
        initiator.query(
            """
            WITH 'SELECT x FROM remote_only_eval_table' AS q
            SELECT *
            FROM remote(remote_eval, database = eval(q))
            SETTINGS allow_experimental_eval_table_function = 1,
                enable_analyzer = 1,
                serialize_query_plan = 1
            """
        )
        == "42\n"
    )


def test_remote_named_collection_database_override_preserves_scalar_eval_udf():
    initiator.query("DROP FUNCTION IF EXISTS eval")
    initiator.query("CREATE FUNCTION eval AS x -> 'system'")

    try:
        assert (
            initiator.query(
                "SELECT count() FROM remote(remote_eval_scalar, database = eval(1))"
            )
            == "1\n"
        )
    finally:
        initiator.query("DROP FUNCTION IF EXISTS eval")


def test_remote_sharding_key_preserves_scalar_eval_udf_argument():
    initiator.query("DROP FUNCTION IF EXISTS eval")
    initiator.query("CREATE FUNCTION eval AS x -> x + 1")
    initiator.query("DROP TABLE IF EXISTS eval_sharding_key_source")
    initiator.query("CREATE TABLE eval_sharding_key_source (q UInt64) ENGINE = Memory")
    initiator.query("INSERT INTO eval_sharding_key_source VALUES (1)")
    remote.query("DROP TABLE IF EXISTS remote_eval_sharding_key_result")
    remote.query("CREATE TABLE remote_eval_sharding_key_result (q UInt64) ENGINE = Memory")

    try:
        initiator.query(
            """
            WITH 10 AS q
            INSERT INTO FUNCTION remote(
                'remote',
                currentDatabase(),
                remote_eval_sharding_key_result,
                throwIf(eval(q) != 2))
            SELECT q FROM eval_sharding_key_source
            SETTINGS enable_analyzer = 0, prefer_column_name_to_alias = 1
            """
        )

        assert remote.query("SELECT * FROM remote_eval_sharding_key_result") == "1\n"
    finally:
        initiator.query("DROP TABLE IF EXISTS eval_sharding_key_source")
        remote.query("DROP TABLE IF EXISTS remote_eval_sharding_key_result")
        initiator.query("DROP FUNCTION IF EXISTS eval")
