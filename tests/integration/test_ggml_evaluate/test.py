import os
import sys
import pathlib

import pytest

from helpers.cluster import ClickHouseCluster


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    stay_alive=True,
    main_configs=["config/models_config.xml"],
)


@pytest.fixture(scope="module")
def ch_cluster():
    try:
        cluster.start()

        os.system(
            "docker cp {local} {cont_id}:{dist}".format(
                local=os.path.join(SCRIPT_DIR, "model/."),
                cont_id=instance.docker_id,
                dist="/etc/clickhouse-server/model",
            )
        )
        instance.restart_clickhouse()

        yield cluster
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------


def testSuspicion(ch_cluster):
    result = instance.query(
        "SELECT ggmlEvaluate('gpt2', map('n_predict', 4), 'No one suspected him');"
    )
    expected = "No one suspected him until shortly after he\n"

    assert result == expected


def testBasicArgumentTypes(ch_cluster):
    err = instance.query_and_get_error(
        "SELECT ggmlEvaluate(123, map('n_predict', 4), 'No one suspected him');"
    )

    assert (
        "Illegal type UInt8 of first argument of function ggmlEvaluate, expected a string"
        in err
    )

    err = instance.query_and_get_error(
        "SELECT ggmlEvaluate('gpt2', 123, 'No one suspected him');"
    )

    assert (
        "Illegal type UInt8 of second argument of function ggmlEvaluate, expected a map"
        in err
    )

    err = instance.query_and_get_error(
        "SELECT ggmlEvaluate('gpt2', map('n_predict', 4), 123);"
    )

    assert (
        "Illegal type UInt8 of third argument of function ggmlEvaluate, expected a string"
        in err
    )


def testFirstArgumentIsConstString(ch_cluster):
    _ = instance.query("DROP TABLE IF EXISTS T;")
    _ = instance.query("CREATE TABLE T (a TEXT) ENGINE MergeTree PRIMARY KEY a;")

    err = instance.query_and_get_error(
        "SELECT ggmlEvaluate(a, map('n_predict', 4), 'No one suspected him') FROM T;"
    )

    assert "First argument of function ggmlEvaluate must be a constant string" in err


def testArgumentCount(ch_cluster):
    err = instance.query_and_get_error("SELECT ggmlEvaluate('gpt2');")

    assert "Function ggmlEvaluate expects exactly 3 arguments. Got 1" in err

    err = instance.query_and_get_error(
        "SELECT ggmlEvaluate('gpt2', map('n_predict', 4));"
    )

    assert "Function ggmlEvaluate expects exactly 3 arguments. Got 2" in err

    err = instance.query_and_get_error(
        "SELECT ggmlEvaluate('gpt2', map('n_predict', 4), 'No one suspected him', ' of anything');"
    )

    assert "Function ggmlEvaluate expects exactly 3 arguments. Got 4" in err


def testMultipleRows(ch_cluster):
    _ = instance.query("DROP TABLE IF EXISTS T;")
    _ = instance.query("CREATE TABLE T (a TEXT) ENGINE MergeTree PRIMARY KEY a;")
    _ = instance.query(
        "INSERT INTO T (a) VALUES ('Actually, proactive'), ('No one suspected him')"
    )

    result = instance.query(
        "SELECT ggmlEvaluate('gpt2', map('n_predict', 3), a) from T;"
    )
    expected = "Actually, proactive data editing choice\nNo one suspected him until shortly after\n"
    assert result == expected


def testTwoConsecutiveEvals(ch_cluster):
    _ = instance.query("DROP TABLE IF EXISTS T;")
    _ = instance.query("CREATE TABLE T (a TEXT) ENGINE MergeTree PRIMARY KEY a;")
    _ = instance.query(
        "INSERT INTO T (a) VALUES ('From the deepest'), ('No one suspected him')"
    )

    result = instance.query(
        "SELECT ggmlEvaluate('gpt2', map('n_predict', 4), ggmlEvaluate('gpt2', map('n_predict', 4), a)) from T;"
    )
    expected = "From the deepest data base architecture thinges until Barclay\nNo one suspected him until shortly after he descended Everest base station\n"
    assert result == expected
