import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=[
        "configs/users.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_user_with_dot_in_name():
    assert node.query("SELECT count()>0 FROM system.users where name = 'user.name'") == "1\n"
    assert node.query("SELECT count()>0 FROM system.users where name = 'user\\.name'") == "0\n"

    node.query("DROP USER IF EXISTS 'foo.bar'")
    node.query("CREATE USER 'foo.bar'")
    assert node.query("SELECT count()>0 FROM system.users where name = 'foo.bar'") == "1\n"
    assert node.query("SELECT count()>0 FROM system.users where name = 'foo\\.bar'") == "0\n"

    node.query("ALTER USER 'foo.bar' RENAME TO 'foo\\.bar'")
    assert node.query("SELECT count()>0 FROM system.users where name = 'foo.bar'") == "0\n"
    assert node.query("SELECT count()>0 FROM system.users where name = 'foo\\.bar'") == "1\n"
    node.query("DROP USER 'foo\\.bar'")
