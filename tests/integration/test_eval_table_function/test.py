import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

initiator = cluster.add_instance("initiator")
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
