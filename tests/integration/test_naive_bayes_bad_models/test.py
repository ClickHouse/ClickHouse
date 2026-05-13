import os
import struct
import tempfile
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CFG_DIR = os.path.join(SCRIPT_DIR, "configs")


def write_good(file_path):
    with open(file_path, "wb") as f:
        f.write(struct.pack("<I", 0))  # class_id
        f.write(struct.pack("<I", 1))  # len
        f.write(b"a")  # n-gram
        f.write(struct.pack("<I", 1))  # count


def write_gibberish(file_path):
    with open(file_path, "wb") as f:
        f.write(b"\xca\xfe\xba")  # gibberish file content


def write_zero_length(file_path):
    with open(file_path, "wb") as f:
        f.write(struct.pack("<I", 0))
        f.write(struct.pack("<I", 0))  # ngram length 0 given
        f.write(b"")
        f.write(struct.pack("<I", 1))


def write_huge_length(file_path):
    with open(file_path, "wb") as f:
        f.write(struct.pack("<I", 0))
        f.write(struct.pack("<I", 0x7FFFFFFF))  # 2,147,483,647 ngram length given
        f.write(b"x")
        f.write(struct.pack("<I", 1))


def write_wrong_types(file_path):
    with open(file_path, "wb") as f:
        f.write(b"CLID")  # four printable chars instead of uint32
        f.write(b"LENG")
        f.write(b"oops")
        f.write(b"CNT!")


def write_empty(file_path):
    open(file_path, "wb").close()  # zero-byte file


writers = {
    "good.bin": write_good,
    "gibberish.bin": write_gibberish,
    "zero_length.bin": write_zero_length,
    "huge_length.bin": write_huge_length,
    "wrong_types.bin": write_wrong_types,
    "empty.bin": write_empty,
}

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
    tf = tempfile.NamedTemporaryFile(prefix=bin_name, suffix=".bin", delete=False)
    tf.close()
    writers[bin_name](tf.name)

    node.copy_file_to_container(
        os.path.join(CFG_DIR, xml_name),
        "/etc/clickhouse-server/config.d/naive_bayes.xml",
    )

    node.copy_file_to_container(
        tf.name,
        f"/etc/clickhouse-server/config.d/{bin_name}",
    )

    node.restart_clickhouse()
    os.unlink(tf.name)


def test_good_model():
    load_model("good.xml", "good.bin")
    assert node.query("SELECT naiveBayesClassifier('good_model', 'a')") == TSV([[0]])


def test_gibberish_model():
    load_model("gibberish.xml", "gibberish.bin")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT naiveBayesClassifier('gibberish_model', 'x')")


def test_zero_length_model():
    load_model("zero_length.xml", "zero_length.bin")
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT naiveBayesClassifier('zero_length_model', 'x')")


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
