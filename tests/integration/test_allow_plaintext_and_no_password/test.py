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
    node.query("DROP USER IF EXISTS u_02207, u1_02207", password="1w2swhb1")
    node.query("CREATE USER u_02207 IDENTIFIED WITH double_sha1_hash BY '8DCDD69CE7D121DE8013062AEAEB2A148910D50E'" + "\n", password="1w2swhb1")
    node.query("CREATE USER u1_02207 IDENTIFIED BY 'qwe123'", password="1w2swhb1")
    node.query("CREATE USER u2_02207 HOST IP '127.1' IDENTIFIED WITH plaintext_password BY 'qwerty' -- { serverError 36 }", password="1w2swhb1")
    node.query("CREATE USER u3_02207 HOST IP '127.1' IDENTIFIED WITH no_password -- { serverError 36 }", password="1w2swhb1")
    node.query("CREATE USER u4_02207 HOST IP '127.1' NOT IDENTIFIED -- { serverError 36 }", password="1w2swhb1")
    node.query("CREATE USER IF NOT EXISTS u5_02207 -- { serverError 36 }", password="1w2swhb1")
    node.query("DROP USER u_02207, u1_02207", password="1w2swhb1")

    assert node.grep_in_log(
        substring="User is not allowed to Create users",
        filename="clickhouse-server.err.log",
    ) == ""
