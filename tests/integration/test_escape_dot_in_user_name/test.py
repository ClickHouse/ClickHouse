import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node_escape_dot = cluster.add_instance(
    "node_escape_dot",
    main_configs=[
        "configs/config.d/escape_dot_in_user_name.xml",
    ],
    user_configs=[
        "configs/users.xml",
    ],
)
node_dont_escape_dot = cluster.add_instance(
    "node_dont_escape_dot",
    main_configs=[
        "configs/config.d/dont_escape_dot_in_user_name.xml",
    ],
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
    assert node_escape_dot.query("SELECT count()>0 FROM system.users where name = 'user.name'") == "0\n"
    assert node_escape_dot.query("SELECT count()>0 FROM system.users where name = 'user\\.name'") == "1\n"

    # Creating user via query works as previously
    node_escape_dot.query("CREATE USER 'foo.bar'")
    assert node_escape_dot.query("SELECT count()>0 FROM system.users where name = 'foo.bar'") == "1\n"
    assert node_escape_dot.query("SELECT count()>0 FROM system.users where name = 'foo\\.bar'") == "0\n"

    node_escape_dot.query("ALTER USER 'foo.bar' RENAME TO 'foo\\.bar'")
    assert node_escape_dot.query("SELECT count()>0 FROM system.users where name = 'foo.bar'") == "0\n"
    assert node_escape_dot.query("SELECT count()>0 FROM system.users where name = 'foo\\.bar'") == "1\n"

    assert node_dont_escape_dot.query("SELECT count()>0 FROM system.users where name = 'user.name'") == "1\n"
    assert node_dont_escape_dot.query("SELECT count()>0 FROM system.users where name = 'user\\.name'") == "0\n"

    node_dont_escape_dot.query("CREATE USER 'baz.quux'")
    assert node_dont_escape_dot.query("SELECT count()>0 FROM system.users where name = 'baz.quux'") == "1\n"
    assert node_dont_escape_dot.query("SELECT count()>0 FROM system.users where name = 'baz\\.quux'") == "0\n"

    node_dont_escape_dot.query("ALTER USER 'baz.quux' RENAME TO 'baz\\.quux'")
    assert node_dont_escape_dot.query("SELECT count()>0 FROM system.users where name = 'baz.quux'") == "0\n"
    assert node_dont_escape_dot.query("SELECT count()>0 FROM system.users where name = 'baz\\.quux'") == "1\n"
