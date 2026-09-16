import base64
import hashlib
import hmac
import os
import re
import struct
import time
import xml.etree.ElementTree as ET

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

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
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

    assert "REQUIRED_SECOND_FACTOR" in node.query_and_get_error(
        query_text, user="totuser", password="aa+bb"
    )

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
        c.send("000000", eol="\r")
        c.expect(expected_error)

    with client(command=f"{client_command(user0)} --password aa+bb+000000") as c:
        c.expect(expected_error)

    with client(command=f"{client_command(user0)} --password wrongpwd") as c:
        c.expect(expected_error)

    with client(
        command=f"{client_command(user0)} --password wrongpwd+{get_otp(user0)}"
    ) as c:
        c.expect(expected_error)


def test_one_time_only_no_password(started_cluster):
    query_text = "SELECT currentUser() || toString(42)"
    user0, user1 = NO_PASSWORD_USERS

    assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
        query_text, user=user0, password="000000"
    )

    assert f"{user0}42\n" == node.query(query_text, user=user0, password=get_otp(user0))

    with client(command=f"{client_command(user0)}") as c:
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
