"""Test that the periodic users-config reload picks up changes to the substitutions
file named by the users config's own `include_from` element.

The users config is loaded by its own `ConfigProcessor`, so its `include_from`
element (not the one of the main server config) determines the substitutions file.
`ConfigReloader` learns that path from the loaded config and must watch it, so that
editing the substitutions file alone triggers a background reload. See #114161.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/users_incl.xml"],
    main_configs=["configs/substitutions.xml"],
)

NEW_SUBSTITUTIONS = """
<clickhouse>
    <mqs>222222</mqs>

    <extra_users>
        <first_user>
            <password></password>
            <profile>default</profile>
        </first_user>
        <second_user>
            <password></password>
            <profile>default</profile>
        </second_user>
    </extra_users>
</clickhouse>
"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_reload_include_from_of_users_config(started_cluster):
    # The initial substitutions are applied to the users config.
    assert node.query("SELECT 1", user="first_user") == "1\n"
    assert (
        node.query("SELECT getSetting('max_query_size')", user="first_user")
        == "111111\n"
    )
    assert "second_user" not in node.query("SELECT name FROM system.users")

    # Change only the substitutions file. No query like SYSTEM RELOAD CONFIG here:
    # the background reload of the users config must notice the change by itself.
    node.replace_config(
        "/etc/clickhouse-server/config.d/substitutions.xml", NEW_SUBSTITUTIONS
    )

    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.users WHERE name = 'second_user'",
        "1",
    )
    assert node.query("SELECT 1", user="second_user") == "1\n"
    assert (
        node.query("SELECT getSetting('max_query_size')", user="first_user")
        == "222222\n"
    )
