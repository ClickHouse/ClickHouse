import logging

import pytest
from redis import Redis, exceptions

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/redis.xml"],
    user_configs=["configs/users.xml"],
)

server_port = 9006


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # cluster.start waits only for the native protocol port.
        # The server opens the Redis compatibility port a bit later.
        node.wait_for_log_line("Redis compatibility protocol")

        node.query(
            """
            CREATE TABLE IF NOT EXISTS name_surname_map
            (
                name String,
                surname String,
                age UInt8,
                city String
            )
            ENGINE = Join(ANY, LEFT, name)
            """
        )
        node.query(
            """
            INSERT INTO name_surname_map (name, surname, age, city) VALUES
            ('Alice', 'Smith', 30, 'New York'),
            ('Bob', 'Johnson', 25, 'Los Angeles'),
            ('Charlie', 'Brown', 28, 'Chicago'),
            ('Diana', 'Williams', 32, 'Houston'),
            ('Ethan', 'Taylor', 23, 'Phoenix'),
            ('Frank', '', 27, '')
            """
        )
        node.query("CREATE USER IF NOT EXISTS pass_user IDENTIFIED WITH plaintext_password BY 'secret'")
        node.query("GRANT SELECT ON default.name_surname_map TO pass_user")
        node.query("CREATE USER IF NOT EXISTS limited_user IDENTIFIED WITH plaintext_password BY 'secret'")
        yield cluster
    except Exception as ex:
        logging.exception(ex)
        raise
    finally:
        cluster.shutdown()


@pytest.fixture()
def redis_client(started_cluster):
    client = Redis(host=started_cluster.get_instance_ip("node"), port=server_port)
    yield client
    client.close()


def test_basic_commands(redis_client):
    assert redis_client.ping()

    value = redis_client.echo("Hello world")
    assert value == b"Hello world"

    # An empty string is a valid bulk string, not Nil.
    assert redis_client.echo("") == b""

    assert redis_client.quit()


def test_select(redis_client):
    with pytest.raises(exceptions.ResponseError) as resp_err:
        redis_client.get("Alice")
    assert "Redis db not set" in str(resp_err.value)

    with pytest.raises(exceptions.ResponseError) as resp_err:
        redis_client.select(42)
    assert "DB index is out of range" in str(resp_err.value)

    assert redis_client.select(0)


def test_hash_db(redis_client):
    assert redis_client.select(0)

    assert redis_client.hget("Alice", "city") == b"New York"
    assert redis_client.hget("Alice", "surname") == b"Smith"
    assert redis_client.hmget("Bob", "surname", "city") == [b"Johnson", b"Los Angeles"]

    # Non-string values are returned in their text representation.
    assert redis_client.hget("Alice", "age") == b"30"

    # A stored empty string is an empty bulk string, not Nil.
    assert redis_client.hget("Frank", "city") == b""

    # Unknown key: sent as Nil.
    assert redis_client.hget("Mark", "surname") is None
    assert redis_client.hget("Mark", "age") is None
    assert redis_client.hmget("Mark", "surname", "age") == [None, None]

    # A hash database does not support string commands.
    with pytest.raises(exceptions.ResponseError):
        redis_client.get("Alice")

    # Unknown column.
    with pytest.raises(exceptions.ResponseError):
        redis_client.hget("Alice", "no_such_column")


def test_string_db(redis_client):
    assert redis_client.select(1)

    assert redis_client.get("Alice") == b"Smith"
    assert redis_client.mget("Alice", "Bob", "Charlie") == [
        b"Smith",
        b"Johnson",
        b"Brown",
    ]

    # A stored empty string is an empty bulk string, not Nil,
    # while an unknown key is sent as Nil.
    assert redis_client.get("Frank") == b""
    assert redis_client.mget("Frank", "Mark", "Alice") == [b"", None, b"Smith"]

    # Unknown key: sent as Nil.
    assert redis_client.get("Mark") is None

    # A string database does not support hash commands.
    with pytest.raises(exceptions.ResponseError):
        redis_client.hget("Alice", "city")


def test_auth(started_cluster):
    ip = started_cluster.get_instance_ip("node")

    # Explicit authentication with a user name and a password.
    client = Redis(host=ip, port=server_port, username="pass_user", password="secret")
    assert client.select(1)
    assert client.get("Alice") == b"Smith"
    client.close()

    # Wrong password: the client sends AUTH on connect and the server rejects it.
    client = Redis(host=ip, port=server_port, username="pass_user", password="wrong")
    with pytest.raises(Exception) as err:
        client.ping()
    assert "Authentication failed" in str(err.value) or "invalid username-password pair" in str(err.value)
    client.close()

    # A user without the SELECT grant on the mapped table cannot read data.
    client = Redis(host=ip, port=server_port, username="limited_user", password="secret")
    assert client.select(1)
    with pytest.raises(exceptions.ResponseError) as resp_err:
        client.get("Alice")
    assert "Not enough privileges" in str(resp_err.value)
    client.close()
