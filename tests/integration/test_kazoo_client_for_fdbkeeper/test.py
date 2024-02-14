import pytest
from helpers.client import CommandRequest
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    with_foundationdb=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_four_letter_word_commands(started_cluster):
    assert cluster.get_kazoo_client("zoo1").command("ruok") == "imok\n"


def test_basic_ops(started_cluster):
    zk = cluster.get_kazoo_client("zoo1")
    zk.create("/test", "test")
    zk.create("/test/a", "xxx")
    zk.create("/test/b/path/to/b", "yyy", makepath=True)

    assert zk.exists("/test/a")
    assert zk.exists("/test/b/path/to/b")
    assert zk.get_children("/test/") == ["a", "b"]

    zk.create("/test/a/1")
    zk.create("/test/a/2")
    zk.create("/test/a/3")
    assert zk.get_children("/test/a") == ["1", "2", "3"]
    zk.delete("/test/a/1")
    assert zk.get_children("/test/a") == ["2", "3"]
    zk.delete("/test/a", recursive=True)
    assert not zk.exists("/test/a")

    with pytest.raises(Exception):
        zk.set("/test/a", "aaa")
    zk.set("/test", "ccc")
    zk.set("/test", b"1234")
    assert zk.get("/test") == (b"1234", None)
