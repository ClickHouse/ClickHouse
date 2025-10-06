import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", user_configs=["configs/users.xml"])


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_user_grants():
    assert instance.query("show grants for user1") == ""
    instance.query("create role role1")
    instance.replace_in_config("/etc/clickhouse-server/users.d/users.xml", "<grants></grants>", "<grants><query>GRANT role1</query></grants>")
    instance.query("system reload users")
    instance.wait_for_log_line("performing update on configuration")
    assert instance.query("show grants for user1") == "GRANT role1 TO user1\n"
