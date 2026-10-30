import base64
import hashlib
import hmac
import os
import re
import socket
import struct
import time
import xml.etree.ElementTree as ET

import psycopg2
import pymysql
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.uclient import client, prompt

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
USERS_CONFIG = os.path.join(SCRIPT_DIR, "config/users.xml")

# The server accepts each code at most once and rejects codes for time steps at or before
# the last accepted one (RFC 6238, Section 5.2). Therefore every test uses dedicated users,
# with at most two logins per user: first with the code for the current time step, then with
# the code for the next time step, which is within the server time tolerance.
INTERACTIVE_USERS = [f"totuser_interactive_{i}" for i in range(3)]
NO_PASSWORD_USERS = [f"totuser_no_password_{i}" for i in range(2)]
EMPTY_PASSWORD_USERS = [f"totuser_empty_password_{i}" for i in range(2)]
MYSQL_USERS = {
    "totuser_mysql_plaintext": "aa+bb",
    "totuser_mysql_double_sha1": "abacaba",
}

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["config/mysql.xml", "config/postgresql.xml"],
    user_configs=["config/users.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def fresh_server(started_cluster):
    # The server keeps the used codes in memory to enforce their single use, so a repeated
    # run of a test (e.g. with --count) would see its codes already consumed.
    # Restart the server to reset that state.
    node.restart_clickhouse()


def get_one_time_password(
    secret, interval=30, digits=6, sha_version=hashlib.sha1, timepoint=None
):
    key = base64.b32decode(secret, casefold=True)
    time_step = int((timepoint or time.time()) / interval)
    msg = struct.pack(">Q", time_step)
    hmac_hash = hmac.new(key, msg, sha_version).digest()
    offset = hmac_hash[-1] & 0x0F
    binary_code = struct.unpack(">I", hmac_hash[offset : offset + 4])[0] & 0x7FFFFFFF
    otp = binary_code % (10**digits)
    return f"{otp:0{digits}d}"


def get_otp(user, offset_steps=0):
    """The code of the user for the current time step shifted by `offset_steps`,
    computed with the TOTP parameters of the user from the users config."""
    params = ET.parse(USERS_CONFIG).find(f"./users/{user}/time_based_one_time_password")
    period = int(params.findtext("period", default="30"))
    return get_one_time_password(
        secret=params.findtext("secret"),
        interval=period,
        digits=int(params.findtext("digits", default="6")),
        sha_version=getattr(
            hashlib, params.findtext("algorithm", default="SHA1").lower()
        ),
        timepoint=time.time() + offset_steps * period,
    )


def client_command(user):
    return (
        f"{cluster.get_client_cmd()} --highlight=0 --host {node.ip_address} -u {user}"
    )


def test_one_time_password(started_cluster):
    query_text = "SELECT currentUser() || toString(42)"

    old_password = get_otp("totuser", offset_steps=-3)
    assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
        query_text, user="totuser", password=f"aa+bb+{old_password}"
    )

    # Without a code the response does not depend on the password, so the error REQUIRED_SECOND_FACTOR
    # does not confirm a guessed password: a wrong or an empty password gets exactly the same error
    error_without_code = node.query_and_get_error(
        query_text, user="totuser", password="aa+bb"
    )
    assert "REQUIRED_SECOND_FACTOR" in error_without_code
    for password in ["wrongpwd", ""]:
        assert error_without_code == node.query_and_get_error(
            query_text, user="totuser", password=password
        )

    http_responses = [
        node.http_request(
            "",
            params={"query": query_text},
            headers={"X-ClickHouse-User": "totuser", "X-ClickHouse-Key": password},
        )
        for password in ["aa+bb", "wrongpwd"]
    ]
    for response in http_responses:
        assert response.status_code == 403
        assert response.headers["X-ClickHouse-Exception-Code"] == "767"
    assert http_responses[0].text == http_responses[1].text

    response = node.http_request(
        "",
        params={"query": query_text},
        headers={"X-ClickHouse-User": "totuser", "X-ClickHouse-Key": "aa+bb+000000000"},
    )
    assert response.status_code == 403
    assert response.headers["X-ClickHouse-Exception-Code"] == "516"

    assert "totuser42\n" == node.query(
        query_text, user="totuser", password=f"aa+bb+{get_otp('totuser')}"
    )

    resp = node.query(
        """
            SELECT
                name,
                auth_type[1],
                JSONExtractString(auth_params[1], 'second_factor'),
                JSONExtractString(auth_params[1], 'otp_algorithm'),
                JSONExtractString(auth_params[1], 'otp_num_digits'),
                JSONExtractString(auth_params[1], 'otp_period'),
            FROM system.users WHERE name = 'totuser'
        """,
        user="totuser",
        password=f"aa+bb+{get_otp('totuser', offset_steps=1)}",
    )
    assert "totuser\tplaintext_password\tone_time_password\tSHA256\t9\t60" in resp


def test_interactive_totp_authentication(started_cluster):
    """Test TOTP authentication in interactive client mode."""
    user0, user1, user2 = INTERACTIVE_USERS

    # Password and TOTP provided in command line arguments
    with client(
        command=f"{client_command(user0)} --password aa+bb+{get_otp(user0)}"
    ) as c:
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect(f"{user0}42")
        c.expect(prompt)

    with client(
        command=f"{client_command(user0)} --password aa+bb --one-time-password {get_otp(user0, offset_steps=1)}"
    ) as c:
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect(f"{user0}42")
        c.expect(prompt)

    # No password provided in command line arguments
    with client(command=f"{client_command(user1)}") as c:
        # Enter password + TOTP when prompted
        c.expect("Password.*:")
        c.send(f"aa+bb+{get_otp(user1)}", eol="\r")
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect(f"{user1}42")
        c.expect(prompt)

    with client(command=f"{client_command(user1)}") as c:
        # Enter password when prompted first
        c.expect("Password.*:")
        c.send("aa+bb", eol="\r")

        # Then enter TOTP when prompted
        c.expect("TOTP.*:")
        c.send(get_otp(user1, offset_steps=1), eol="\r")
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect(f"{user1}42")
        c.expect(prompt)

    # Password provided in command line arguments, then only TOTP prompted
    with client(command=f"{client_command(user2)} --password aa+bb") as c:
        c.expect("TOTP.*:")
        c.send(get_otp(user2), eol="\r")
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect(f"{user2}42")
        c.expect(prompt)

    with client(
        command=f"{client_command(user2)} --one-time-password {get_otp(user2, offset_steps=1)}"
    ) as c:
        # Enter only password, TOTP is provided in command line arguments
        c.expect("Password.*:")
        c.send("aa+bb", eol="\r")
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect(f"{user2}42")
        c.expect(prompt)

    # Errors:
    expected_error = re.compile(r"Authentication failed|password is incorrect")

    with client(command=f"{client_command(user0)}") as c:
        c.expect("Password.*:")
        c.send("aa+bb", eol="\r")

        # Then enter wrong TOTP when prompted
        c.expect("TOTP.*:")
        c.send("000000000", eol="\r")
        c.expect(expected_error)

    # A wrong code of the configured length (9 digits); a suffix of another length counts as no code
    with client(command=f"{client_command(user0)} --password aa+bb+000000000") as c:
        c.expect(expected_error)

    # A wrong password without a code is asked for the code like a right one, and fails only after that
    with client(command=f"{client_command(user0)} --password wrongpwd") as c:
        c.expect("TOTP.*:")
        c.send(get_otp(user0), eol="\r")
        c.expect(expected_error)

    with client(
        command=f"{client_command(user0)} --password wrongpwd+{get_otp(user0)}"
    ) as c:
        c.expect(expected_error)


def mysql_connect(user, password):
    return pymysql.connections.Connection(
        host=node.ip_address,
        user=user,
        password=password,
        database="default",
        port=9004,
    )


def test_mysql_protocol_requires_totp(started_cluster):
    """The MySQL protocol must enforce TOTP: the `mysql_native_password` auth response is a hash
    of the password alone and cannot carry a one-time password, so the server switches such
    users to the `sha256_password` plugin and the client appends the TOTP to the password.
    """

    for user, password in MYSQL_USERS.items():
        # The correct password alone must not authenticate: the second factor is required.
        # The message is intentionally generic; only the error code tells the reason.
        with pytest.raises(pymysql.err.MySQLError) as exc_info:
            mysql_connect(user, password)
        assert exc_info.value.args[0] == 767  # REQUIRED_SECOND_FACTOR

        # A wrong one-time password must not authenticate.
        with pytest.raises(pymysql.err.MySQLError, match="Authentication failed"):
            mysql_connect(user, password + "+000000000")

        conn = mysql_connect(user, f"{password}+{get_otp(user)}")
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT currentUser()")
            assert cursor.fetchall() == ((user,),)
        finally:
            conn.close()


def test_mysql_protocol_fail_close_on_unverifiable_methods(started_cluster):
    """`IAccessStorage::authenticateImpl` fails close for ambiguous credentials: when the same
    password is accepted by several methods, the session expires at the earliest of their
    `VALID UNTIL` and is limited to the intersection of their `GRANTS`. The `mysql_native_password`
    auth response can only be re-checked against `plaintext_password` and `double_sha1_password`
    methods, so a `scram_sha256_password` or `bcrypt_password` method that narrows the session
    would silently drop out of that combination. Such users are switched to the `sha256_password`
    plugin, which transmits the actual password, so every method takes part in the check.
    """

    for user, other_method in (
        ("mysql_double_sha1_and_scram", "scram_sha256_password"),
        ("mysql_double_sha1_and_bcrypt", "bcrypt_password"),
    ):
        node.query(
            f"CREATE USER {user} IDENTIFIED WITH double_sha1_password BY 'pw', "
            f"{other_method} BY 'pw' VALID UNTIL '2000-01-01 00:00:00'"
        )
        try:
            # The native protocol rejects the shared password as expired ...
            assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
                "SELECT 1", user=user, password="pw"
            )
            # ... and so must the MySQL protocol.
            with pytest.raises(pymysql.err.MySQLError, match="Authentication failed"):
                mysql_connect(user, "pw")

            # Without the narrowing method the user is served by `mysql_native_password` again.
            node.query(
                f"ALTER USER {user} IDENTIFIED WITH double_sha1_password BY 'pw'"
            )
            conn = mysql_connect(user, "pw")
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT currentUser()")
                assert cursor.fetchall() == ((user,),)
            finally:
                conn.close()
        finally:
            node.query(f"DROP USER IF EXISTS {user}")

    # A user with only such a method could not log in over MySQL at all before: now `sha256_password` serves it.
    node.query(
        "CREATE USER mysql_scram_only IDENTIFIED WITH scram_sha256_password BY 'pw'"
    )
    try:
        conn = mysql_connect("mysql_scram_only", "pw")
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT currentUser()")
            assert cursor.fetchall() == (("mysql_scram_only",),)
        finally:
            conn.close()
    finally:
        node.query("DROP USER IF EXISTS mysql_scram_only")


def mysql_read_packet(sock):
    def read_exactly(n):
        data = b""
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            assert chunk, "connection closed by the server"
            data += chunk
        return data

    header = read_exactly(4)
    payload_size = int.from_bytes(header[:3], "little")
    return header[3], read_exactly(payload_size)


def mysql_write_packet(sock, sequence_id, payload):
    sock.sendall(len(payload).to_bytes(3, "little") + bytes([sequence_id]) + payload)


def mysql_native_password_scramble(password, salt):
    password_sha1 = hashlib.sha1(password.encode()).digest()
    digest = hashlib.sha1(salt + hashlib.sha1(password_sha1).digest()).digest()
    return bytes(a ^ b for a, b in zip(password_sha1, digest))


def test_mysql_protocol_user_altered_during_authentication(started_cluster):
    """The plugin is chosen from the user before the client answers, and the user can be altered meanwhile.
    The `mysql_native_password` response must still be rejected if the user authenticated against has
    a method that this plugin cannot enforce. The client is paused deterministically: it asks for
    `sha256_password`, so the server sends an `AuthSwitchRequest` to `mysql_native_password` and waits.
    """

    user = "mysql_altered_during_auth"
    node.query(f"CREATE USER {user} IDENTIFIED WITH double_sha1_password BY 'old'")
    try:
        with socket.create_connection((node.ip_address, 9004), timeout=30) as sock:
            mysql_read_packet(sock)  # Initial handshake.

            CLIENT_PROTOCOL_41 = 0x200
            CLIENT_SECURE_CONNECTION = 0x8000
            CLIENT_PLUGIN_AUTH = 0x80000
            capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
            handshake_response = (
                struct.pack("<IIB", capabilities, 16 * 1024 * 1024, 33)
                + b"\0" * 23
                + user.encode() + b"\0"
                + b"\0"  # Empty auth response.
                + b"sha256_password\0"
            )
            mysql_write_packet(sock, 1, handshake_response)

            sequence_id, auth_switch_request = mysql_read_packet(sock)
            assert auth_switch_request[0] == 0xFE
            plugin_name, salt = auth_switch_request[1:].split(b"\0", 1)
            assert plugin_name == b"mysql_native_password"

            # The server waits for the client here.
            node.query(
                f"ALTER USER {user} IDENTIFIED WITH double_sha1_password BY 'new', "
                "bcrypt_password BY 'new' VALID UNTIL '2000-01-01 00:00:00'"
            )

            mysql_write_packet(
                sock, sequence_id + 1, mysql_native_password_scramble("new", salt[:20])
            )
            _, result = mysql_read_packet(sock)
            assert result[0] == 0xFF, "the login must be rejected"
            assert b"Authentication failed" in result
    finally:
        node.query(f"DROP USER IF EXISTS {user}")


def test_postgresql_protocol_requires_totp(started_cluster):
    """The PostgreSQL protocol must enforce TOTP. A SCRAM proof is derived from the password
    alone and cannot carry a one-time password, so the server refuses the SCRAM exchange for
    a `scram_sha256_password` user with TOTP. For cleartext password authentication the client
    appends the TOTP to the password."""

    def connect(user, password):
        return psycopg2.connect(
            host=node.ip_address,
            port=9005,
            user=user,
            password=password,
            dbname="default",
        )

    # A SCRAM user with TOTP must be refused: the exchange cannot verify the second factor.
    scram_user = "totuser_postgresql_scram"
    with pytest.raises(psycopg2.OperationalError, match="not supported"):
        connect(scram_user, "abacaba")

    with pytest.raises(psycopg2.OperationalError, match="not supported"):
        connect(scram_user, f"abacaba+{get_otp(scram_user)}")

    # Cleartext password authentication enforces the second factor.
    # The message is intentionally generic and does not reveal the reason.
    user = "totuser_postgresql_plaintext"
    with pytest.raises(psycopg2.OperationalError, match="Invalid user or password"):
        connect(user, "aa+bb")

    with pytest.raises(psycopg2.OperationalError, match="Invalid user or password"):
        connect(user, "aa+bb+000000000")

    conn = connect(user, f"aa+bb+{get_otp(user)}")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT currentUser()")
        assert cursor.fetchall() == [(user,)]
    finally:
        conn.close()


def test_one_time_only_no_password(started_cluster):
    query_text = "SELECT currentUser() || toString(42)"
    user0, user1 = NO_PASSWORD_USERS

    assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
        query_text, user=user0, password="000000"
    )

    assert f"{user0}42\n" == node.query(query_text, user=user0, password=get_otp(user0))

    # The client cannot know that the account has no password: it asks for the password first
    with client(command=f"{client_command(user0)}") as c:
        c.expect("Password.*:")
        c.send("", eol="\r")
        c.expect("TOTP.*:")
        c.send(get_otp(user0, offset_steps=1), eol="\r")
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect(f"{user0}42")
        c.expect(prompt)

    # When main password is empty TOTP works in both places:
    with client(command=f"{client_command(user1)} --password {get_otp(user1)}") as c:
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect(f"{user1}42")
        c.expect(prompt)

    with client(
        command=f"{client_command(user1)} --one-time-password {get_otp(user1, offset_steps=1)}"
    ) as c:
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect(f"{user1}42")
        c.expect(prompt)


def test_empty_password_with_otp_cli_option(started_cluster):
    """Test that --one-time-password works for a user with empty plaintext password and TOTP."""
    query_text = "SELECT currentUser() || toString(42)"
    user0, user1 = EMPTY_PASSWORD_USERS

    assert f"{user0}42\n" == node.query(
        query_text, user=user0, password=f"+{get_otp(user0)}"
    )

    with client(
        command=f"{client_command(user0)} --one-time-password {get_otp(user0, offset_steps=1)}"
    ) as c:
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect(f"{user0}42")
        c.expect(prompt)

    with client(
        command=f'{client_command(user1)} --password "" --one-time-password {get_otp(user1)}'
    ) as c:
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect(f"{user1}42")
        c.expect(prompt)


def test_code_single_use(started_cluster):
    """Each code is accepted at most once (RFC 6238, Section 5.2), and codes for time steps
    at or before the last accepted one are rejected."""
    query_text = "SELECT currentUser() || toString(42)"
    user = "totuser_single_use"

    # A failed attempt (wrong password with a valid code) does not consume the code
    code = get_otp(user)
    assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
        query_text, user=user, password=f"wrongpwd+{code}"
    )
    assert f"{user}42\n" == node.query(query_text, user=user, password=f"pw+{code}")

    # The same code is rejected on the second use, from any interface
    assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
        query_text, user=user, password=f"pw+{code}"
    )
    assert "AUTHENTICATION_FAILED" in node.http_query_and_get_error(
        query_text, user=user, password=f"pw+{code}"
    )

    # A code for an earlier time step than the last accepted one is rejected even if it was never used
    assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
        query_text, user=user, password=f"pw+{get_otp(user, offset_steps=-1)}"
    )

    # The code for the next time step is within the server time tolerance: accepted once, then rejected
    next_code = get_otp(user, offset_steps=1)
    assert f"{user}42\n" == node.query(
        query_text, user=user, password=f"pw+{next_code}"
    )
    assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
        query_text, user=user, password=f"pw+{next_code}"
    )


def test_secret_rotation_on_reload(started_cluster):
    """A changed secret in the users config applies on `SYSTEM RELOAD USERS`."""
    query_text = "SELECT currentUser() || toString(42)"
    user = "totuser_rotation"
    config_path = "/etc/clickhouse-server/users.d/users.xml"
    old_secret = "MFRGGZDFMZTWQ2LK"
    new_secret = "QTUD6Q3PJBFU2OMU"

    node.replace_in_config(config_path, old_secret, new_secret)
    try:
        node.query("SYSTEM RELOAD USERS")

        assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
            query_text, user=user, password=f"pw+{get_otp(user)}"
        )
        assert f"{user}42\n" == node.query(
            query_text, user=user, password=f"pw+{get_one_time_password(new_secret)}"
        )
    finally:
        node.replace_in_config(config_path, new_secret, old_secret)
        node.query("SYSTEM RELOAD USERS")
