import pytest
import logging

from helpers.cluster import ClickHouseCluster
from redis import Redis

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", 
    main_configs=[
        "configs/config.xml",
    ], 
    user_configs=[
        "configs/users.xml",
    ]
)

server_port = 9006

@pytest.fixture(scope="module")
def server_address():
    try:
        cluster.start()
        # Wait for the Redis handler to start.
        # Cluster.start waits until port 9000 becomes accessible.
        # Server opens the Redis compatibility port a bit later.
        cluster.instances["node"].wait_for_log_line("Redis compatibility protocol")
        yield cluster.get_instance_ip("node")
    except Exception as ex:
            logging.exception(ex)
            raise ex
    finally:
        cluster.shutdown()

def test_python_client(server_address):
    redis = Redis(host=server_address, port=server_port)

    value = redis.ping()
    assert value == "PONG"

    value = redis.echo("Hello")
    assert value == "Hello"