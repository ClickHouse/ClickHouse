import os
import sys

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
            "wget --quiet -O {gpt2_path} {gpt2_download_link}".format(
                gpt2_path=os.path.join(SCRIPT_DIR, "model/gpt2.ggml"),
                gpt2_download_link="https://huggingface.co/ggerganov/ggml/resolve/main/ggml-model-gpt-2-117M.bin?download=true",
            )
        )
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


def testWalruses(ch_cluster):
    result = instance.query(
        "SELECT ggmlEvaluate('gpt2', map('n_predict', 4), 'Did you know that walruses');"
    )
    expected = "Did you know that walruses are actually mammals?\n"

    assert result == expected


def testBasicArgumentTypes(ch_cluster):
    err = instance.query_and_get_error(
        "SELECT ggmlEvaluate(123, map('n_predict', 4), 'Did you know that walruses');"
    )

    assert (
        "Illegal type UInt8 of first argument of function ggmlEvaluate, expected a string"
        in err
    )

    err = instance.query_and_get_error(
        "SELECT ggmlEvaluate('gpt2', 123, 'Did you know that walruses');"
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
        "SELECT ggmlEvaluate(a, map('n_predict', 4), 'Did you know that walruses') FROM T;"
    )

    assert 'First argument of function ggmlEvaluate must be a constant string' in err


def testArgumentCount(ch_cluster):
    err = instance.query_and_get_error("SELECT ggmlEvaluate('gpt2');")

    assert 'Function ggmlEvaluate expects exactly 3 arguments. Got 1' in err

    err = instance.query_and_get_error(
        "SELECT ggmlEvaluate('gpt2', map('n_predict', 4));"
    )

    assert 'Function ggmlEvaluate expects exactly 3 arguments. Got 2' in err

    err = instance.query_and_get_error(
        "SELECT ggmlEvaluate('gpt2', map('n_predict', 4), 'Did you know that walruses', ' are actually mammals');"
    )

    assert 'Function ggmlEvaluate expects exactly 3 arguments. Got 4' in err


def testMultipleRows(ch_cluster):
    _ = instance.query("DROP TABLE IF EXISTS T;")
    _ = instance.query("CREATE TABLE T (a TEXT) ENGINE MergeTree PRIMARY KEY a;")
    _ = instance.query(
        "INSERT INTO T (a) VALUES ('Did you know that walruses'), ('No one suspected him')"
    )

    result = instance.query(
        "SELECT ggmlEvaluate('gpt2', map('n_predict', 4), a) from T;"
    )
    expected = "Did you know that walruses are actually mammals?\nNo one suspected him of anything except that\n"
    assert result == expected


def testTwoConsecutiveEvals(ch_cluster):
    _ = instance.query("DROP TABLE IF EXISTS T;")
    _ = instance.query("CREATE TABLE T (a TEXT) ENGINE MergeTree PRIMARY KEY a;")
    _ = instance.query(
        "INSERT INTO T (a) VALUES ('Did you know that walruses'), ('No one suspected him')"
    )

    result = instance.query(
        "SELECT ggmlEvaluate('gpt2', map('n_predict', 8), ggmlEvaluate('gpt2', map('n_predict', 4), a)) from T;"
    )
    expected = "Did you know that walruses are actually mammals? I don\\'t know that they\\'re living\nNo one suspected him of anything except that he was one of the most talented athletes\n"
    assert result == expected
