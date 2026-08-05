import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["configs/overrides.yaml"], user_configs=["users/overrides.yaml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def test(start_cluster):
    node.query("DROP USER IF EXISTS u1_02422, u2_02422, u3_02422", password="1w2swhb1")
    node.query("CREATE USER u1_02422 -- { serverError 36 }", password="1w2swhb1")
    node.query("CREATE USER u2_02422 IDENTIFIED WITH no_password", password="1w2swhb1")
    node.query("CREATE USER u3_02422 IDENTIFIED BY 'qwe123'", password="1w2swhb1")
    node.query("DROP USER u2_02422, u3_02422", password="1w2swhb1")

    assert node.grep_in_log(
        substring="User is not allowed to Create users",
        filename="clickhouse-server.err.log",
    ) == ""
