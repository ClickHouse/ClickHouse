import logging
import socket

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
        # An unsupported `Join` variant: `joinGet` only works for `ANY LEFT`.
        node.query(
            """
            CREATE TABLE IF NOT EXISTS all_join_map (name String, surname String)
            ENGINE = Join(ALL, LEFT, name)
            """
        )
        # A value column whose type cannot be wrapped in `Nullable`, so a missing
        # key would not be representable as Nil.
        node.query(
            """
            CREATE TABLE IF NOT EXISTS array_value_map (name String, tags Array(String))
            ENGINE = Join(ANY, LEFT, name)
            """
        )
        # A `LowCardinality` value column has to become `LowCardinality(Nullable(String))`,
        # because `LowCardinality(String)` cannot be wrapped in `Nullable`.
        node.query(
            """
            CREATE TABLE IF NOT EXISTS lc_value_map (name String, surname LowCardinality(String))
            ENGINE = Join(ANY, LEFT, name)
            """
        )
        node.query("INSERT INTO lc_value_map VALUES ('Alice', 'Smith'), ('Frank', '')")
        node.query("CREATE USER IF NOT EXISTS policy_user IDENTIFIED WITH plaintext_password BY 'secret'")
        node.query("GRANT SELECT ON default.name_surname_map TO policy_user")
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


def test_unsupported_join_variant(redis_client):
    # `Join(ALL, LEFT, ...)` cannot serve lookups, and this has to be reported
    # by `SELECT`, not by the first `GET`.
    with pytest.raises(exceptions.ResponseError) as resp_err:
        redis_client.select(3)
    assert "does not support get requests" in str(resp_err.value)


def test_unrepresentable_value_column(redis_client):
    # An `Array` value cannot be wrapped in `Nullable`, so a missing key could not be
    # distinguished from a present one. This has to be reported by `SELECT`, not by
    # the first `GET`.
    with pytest.raises(exceptions.ResponseError) as resp_err:
        redis_client.select(5)
    assert "cannot be used for get requests" in str(resp_err.value)


def test_table_is_resolved_for_every_request(started_cluster, redis_client):
    node.query(
        """
        CREATE TABLE recreated_map (name String, surname String)
        ENGINE = Join(ANY, LEFT, name)
        """
    )
    node.query("INSERT INTO recreated_map VALUES ('Alice', 'Smith')")

    assert redis_client.select(2)
    assert redis_client.get("Alice") == b"Smith"

    # Recreating the table with different data must be visible to the already connected client.
    node.query("DROP TABLE recreated_map SYNC")
    node.query(
        """
        CREATE TABLE recreated_map (name String, surname String)
        ENGINE = Join(ANY, LEFT, name)
        """
    )
    node.query("INSERT INTO recreated_map VALUES ('Alice', 'Jones')")
    assert redis_client.get("Alice") == b"Jones"

    # A dropped table must not keep serving data either.
    node.query("DROP TABLE recreated_map SYNC")
    with pytest.raises(exceptions.ResponseError) as resp_err:
        redis_client.get("Alice")
    assert "recreated_map" in str(resp_err.value)


def test_mapping_is_reread_after_reload_config(started_cluster, redis_client):
    for table, surname in (("reload_map_before", "Smith"), ("reload_map_after", "Jones")):
        node.query(
            f"""
            CREATE TABLE {table} (name String, surname String)
            ENGINE = Join(ANY, LEFT, name)
            """
        )
        node.query(f"INSERT INTO {table} VALUES ('Alice', '{surname}')")

    assert redis_client.select(4)
    assert redis_client.get("Alice") == b"Smith"

    config_path = "/etc/clickhouse-server/config.d/redis.xml"
    node.replace_in_config(config_path, "reload_map_before", "reload_map_after")
    try:
        node.query("SYSTEM RELOAD CONFIG")

        # The new mapping has to be visible to the already connected client: the listener is not
        # restarted by `SYSTEM RELOAD CONFIG` as long as the port stays the same.
        assert redis_client.get("Alice") == b"Jones"

        # And to a newly connected client as well.
        new_client = Redis(host=started_cluster.get_instance_ip("node"), port=server_port)
        try:
            assert new_client.select(4)
            assert new_client.get("Alice") == b"Jones"
        finally:
            new_client.close()

        # Removing the mapping stops exposing the table.
        node.replace_in_config(config_path, "<_4>", "<_200>")
        node.replace_in_config(config_path, "</_4>", "</_200>")
        node.query("SYSTEM RELOAD CONFIG")
        with pytest.raises(exceptions.ResponseError) as resp_err:
            redis_client.get("Alice")
        assert "Redis database 4 is not configured" in str(resp_err.value)
    finally:
        node.replace_in_config(config_path, "<_200>", "<_4>")
        node.replace_in_config(config_path, "</_200>", "</_4>")
        node.replace_in_config(config_path, "reload_map_after", "reload_map_before")
        node.query("SYSTEM RELOAD CONFIG")
        for table in ("reload_map_before", "reload_map_after"):
            node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_mget_returns_values_of_all_keys(redis_client):
    # All the keys of one `MGET` are looked up in a single request, in the order they were sent.
    assert redis_client.select(1)
    assert redis_client.mget("Bob", "Alice", "Mark", "Bob", "Frank") == [
        b"Johnson",
        b"Smith",
        None,
        b"Johnson",
        b"",
    ]


def send_raw_request(started_cluster, request):
    sock = socket.create_connection((started_cluster.get_instance_ip("node"), server_port), timeout=30)
    try:
        sock.sendall(request)
        return sock.recv(4096)
    finally:
        sock.close()


def test_oversized_length_prefixes(started_cluster):
    # The array length is client-controlled and must be rejected before anything is allocated for it.
    response = send_raw_request(started_cluster, b"*100000000\r\n$4\r\nMGET\r\n")
    assert response.startswith(b"-ERR ")
    assert b"exceeds the maximum allowed" in response

    # The same for the length of a bulk string.
    response = send_raw_request(started_cluster, b"*2\r\n$3\r\nGET\r\n$1000000000\r\n")
    assert response.startswith(b"-ERR ")
    assert b"exceeds the maximum allowed" in response


def test_many_tiny_keys_are_rejected(started_cluster):
    # A command with a huge number of tiny arguments is small on the wire, but every argument is
    # held several times over on the server, so the number of elements is limited on its own.
    # The header alone must be enough for the rejection: nothing of the payload is read.
    response = send_raw_request(started_cluster, b"*1048576\r\n$4\r\nMGET\r\n" + b"$1\r\na\r\n" * 16)
    assert response.startswith(b"-ERR ")
    assert b"exceeds the maximum allowed" in response

    # A command that stays under the element limit is still accepted.
    keys = 1024
    request = b"*%d\r\n$4\r\nMGET\r\n" % (keys + 1) + b"$1\r\na\r\n" * keys
    sock = socket.create_connection(
        (started_cluster.get_instance_ip("node"), server_port), timeout=30
    )
    try:
        sock.sendall(b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n" + request)
        response = b""
        # `SELECT` answers `+OK`, `MGET` answers an array of Nils, one per key.
        while response.count(b"\r\n") < keys + 2:
            chunk = sock.recv(65536)
            assert chunk
            response += chunk
    finally:
        sock.close()
    assert response.startswith(b"+OK\r\n*%d\r\n" % keys)
    assert b"-ERR " not in response


def test_low_cardinality_value_column(redis_client):
    # `LowCardinality(String)` is a common type for values, and a missing key still has to be Nil.
    assert redis_client.select(6)
    assert redis_client.get("Alice") == b"Smith"
    assert redis_client.get("Frank") == b""
    assert redis_client.get("Mark") is None
    assert redis_client.mget("Alice", "Mark", "Frank") == [b"Smith", None, b""]


def test_row_policy_is_rejected(started_cluster):
    # A lookup goes straight to the storage and does not evaluate the row policy filter,
    # so a table the user has a row policy on must not be served at all.
    node.query(
        "CREATE ROW POLICY redis_policy ON default.name_surname_map USING name = 'Alice' TO policy_user"
    )
    try:
        client = Redis(
            host=started_cluster.get_instance_ip("node"),
            port=server_port,
            username="policy_user",
            password="secret",
        )
        try:
            with pytest.raises(exceptions.ResponseError) as resp_err:
                client.select(1)
            assert "row policy is applied" in str(resp_err.value)
        finally:
            client.close()

        # A user the policy is not for does not see the rows either, exactly as with a regular
        # `SELECT`: a table with row policies is hidden from everyone they do not apply to.
        with pytest.raises(exceptions.ResponseError) as resp_err:
            redis_client_get(started_cluster, 1, "Alice")
        assert "row polic" in str(resp_err.value)
    finally:
        node.query("DROP ROW POLICY redis_policy ON default.name_surname_map")

    # Once the policy is gone, the table is served again.
    assert redis_client_get(started_cluster, 1, "Alice") == b"Smith"


def redis_client_get(started_cluster, db, key):
    client = Redis(host=started_cluster.get_instance_ip("node"), port=server_port)
    try:
        assert client.select(db)
        return client.get(key)
    finally:
        client.close()


def test_error_does_not_desynchronize_connection(started_cluster):
    # An error response must consume the whole command: otherwise the unread arguments would be
    # interpreted as the beginning of the next command and every subsequent reply would be bogus.
    sock = socket.create_connection((started_cluster.get_instance_ip("node"), server_port), timeout=30)
    try:
        # An unsupported command with arguments.
        sock.sendall(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
        assert sock.recv(4096).startswith(b"-ERR ")

        # The connection is still usable.
        sock.sendall(b"*1\r\n$4\r\nPING\r\n")
        assert sock.recv(4096) == b"+PONG\r\n"

        # A known command with a wrong number of arguments.
        sock.sendall(b"*3\r\n$3\r\nGET\r\n$1\r\nk\r\n$5\r\nextra\r\n")
        assert sock.recv(4096).startswith(b"-ERR ")

        sock.sendall(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
        assert sock.recv(4096) == b"$5\r\nhello\r\n"
    finally:
        sock.close()
