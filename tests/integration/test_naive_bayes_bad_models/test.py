import os
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CFG_DIR = os.path.join(SCRIPT_DIR, "configs")
BIN_DIR = os.path.join(SCRIPT_DIR, "models")

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def load_model(xml_name: str, bin_name: str):
    node.copy_file_to_container(
        os.path.join(CFG_DIR, xml_name),
        "/etc/clickhouse-server/config.d/naive_bayes.xml",
    )

    node.copy_file_to_container(
        os.path.join(BIN_DIR, bin_name),
        f"/etc/clickhouse-server/config.d/{bin_name}",
    )

    node.restart_clickhouse()


def test_good_model():
    load_model("good.xml", "good.bin")
    assert node.query("SELECT naiveBayesClassifier('good_model', 'a')") == TSV([[0]])


def test_gibberish_model():
    load_model("gibberish.xml", "gibberish.bin")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT naiveBayesClassifier('gibberish_model', 'x')")


def test_huge_length_model():
    load_model("huge_length.xml", "huge_length.bin")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT naiveBayesClassifier('huge_length_model', 'x')")


def test_wrong_types_model():
    load_model("wrong_types.xml", "wrong_types.bin")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT naiveBayesClassifier('wrong_types_model', 'x')")


def test_empty_model():
    load_model("empty.xml", "empty.bin")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT naiveBayesClassifier('empty_model', 'x')")
