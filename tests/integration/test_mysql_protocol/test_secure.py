import pymysql
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/ssl_conf.xml",
        "configs/mysql_secure.xml",
        "configs/dhparam.pem",
        "configs/server.crt",
        "configs/server.key",
    ],
    user_configs=["configs/users.xml"],
    env_variables={"UBSAN_OPTIONS": "print_stacktrace=1"},
)

server_port = 9001


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.shutdown()


def test_secure(started_cluster):
    connection = pymysql.connect(
        user="default",
        password="123",
        database="default",
        host=started_cluster.get_instance_ip("node"),
        port=server_port,
        ssl_disabled=False,
        ssl_verify_identity=True,
    )
    try:
        cur = connection.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
    finally:
        connection.close()


def test_cannot_connect_insecure(started_cluster):
    with pytest.raises(pymysql.err.OperationalError) as e:
        pymysql.connect(
            user="default",
            password="abacab",
            database="default",
            host=started_cluster.get_instance_ip("node"),
            port=server_port,
            ssl_disabled=True,
        )
    assert e.value.args == (
        3159,
        "Connections using insecure transport are prohibited.",
    )
