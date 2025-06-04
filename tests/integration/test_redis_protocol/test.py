import pytest
import logging
import os

from helpers.cluster import ClickHouseCluster
from redis import Redis, exceptions
from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[],
    user_configs=["configs/users.xml"],
    env_variables={"UBSAN_OPTIONS": "print_stacktrace=1"},
)

server_port = 9006
     
@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # Wait for the Redis handler to start.
        # Cluster.start waits until port 9006 becomes accessible.
        # Server opens the Redis compatibility port a bit later.
        cluster.instances["node"].wait_for_log_line("Redis compatibility protocol")
        yield cluster
    except Exception as ex:
            logging.exception(ex)
            raise ex
    finally:
        cluster.shutdown()

def setup_join_table():

    node.query("""
    -- Create a Join table
    CREATE TABLE IF NOT EXISTS name_surname_map
    (
        name String,
        surname String,
        age UInt8,
        city String
    )
    ENGINE = Join(ANY, LEFT, name);
    """)

    node.query("""
    INSERT INTO name_surname_map (name, surname, age, city) VALUES
    ('Alice', 'Smith', 30, 'New York'),
    ('Bob', 'Johnson', 25, 'Los Angeles'),
    ('Charlie', 'Brown', 28, 'Chicago'),
    ('Diana', 'Williams', 32, 'Houston'),
    ('Ethan', 'Taylor', 23, 'Phoenix');
    """)
         

def test_python_client_basics(started_cluster):
    redis = Redis(host=started_cluster.get_instance_ip("node"), port=server_port)

    assert redis.ping()
    
    value = redis.echo("Hello world")
    assert value == b'Hello world'
    
    assert redis.quit()

def test_python_client_select(started_cluster):
    setup_join_table()
    redis = Redis(host=started_cluster.get_instance_ip("node"), port=server_port)

    with pytest.raises(exceptions.ResponseError) as resp_err:
        redis.get("MyKey")
    assert str(resp_err.value) == "Redis db not set"

    assert redis.select(0)
    assert redis.quit()


def test_python_client_join_hash(started_cluster):
    setup_join_table()
    redis = Redis(host=started_cluster.get_instance_ip("node"), port=server_port)

    assert redis.select(0)

    city = redis.hget('Alice', 'city')
    assert city == b'New York'

    surname = redis.hget('Alice', 'surname')
    assert surname == b"Smith"

    surname = redis.hget('Mark', 'surname')
    assert (surname is None)

    assert redis.quit()