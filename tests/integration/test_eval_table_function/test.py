import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

initiator = cluster.add_instance("initiator")
remote = cluster.add_instance("remote")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_remote_eval_resolves_generated_query_on_remote():
    remote.query("CREATE TABLE remote_only_eval_table (x UInt64) ENGINE = Memory")
    remote.query("INSERT INTO remote_only_eval_table VALUES (42)")

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
