import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["configs/overrides.yaml"], user_configs=["users/overrides.yaml"], stay_alive=True)


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
    node.replace_in_config("/etc/clickhouse-server/config.d/overrides.yaml", "allow_no_password: 0", "allow_no_password: 1")
    node.restart_clickhouse()

    node.query("DROP USER IF EXISTS u_3574", user="admin", password="1w2swhb1")
    node.query("CREATE USER u_3574 IDENTIFIED WITH no_password", user="admin", password="1w2swhb1")
    assert node.query("SELECT user()", user="u_3574").strip() == "u_3574"

    node.replace_in_config("/etc/clickhouse-server/config.d/overrides.yaml", "allow_no_password: 1", "allow_no_password: 0")
    node.restart_clickhouse()
    with pytest.raises(Exception) as err:
        node.query("SELECT user()", user="u_3574")
    assert "u_3574: Authentication failed: password is incorrect, or there is no user with such name." in str(err.value)

    assert node.grep_in_log(
        substring="User is not allowed to Create users",
        filename="clickhouse-server.err.log",
    ) == ""
