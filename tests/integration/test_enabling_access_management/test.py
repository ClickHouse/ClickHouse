import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', user_configs=["configs/users.d/extra_users.xml"])


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_enabling_access_management():
    instance.query("CREATE USER Alex", user='default')
    assert instance.query("SHOW CREATE USER Alex", user='default') == "CREATE USER Alex\n"
    assert instance.query("SHOW CREATE USER Alex", user='readonly') == "CREATE USER Alex\n"
    assert "Not enough privileges" in instance.query_and_get_error("SHOW CREATE USER Alex", user='xyz')

    assert "Cannot execute query in readonly mode" in instance.query_and_get_error("CREATE USER Robin", user='readonly')
    assert "Not enough privileges" in instance.query_and_get_error("CREATE USER Robin", user='xyz')
