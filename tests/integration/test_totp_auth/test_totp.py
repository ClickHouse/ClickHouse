import base64
import hashlib
import hmac
import os
import random
import re
import struct
import time
from fnmatch import fnmatch

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.uclient import client, prompt

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TOTP_SECRET = base64.b32encode(random.randbytes(random.randint(3, 64))).decode()

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["config/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        create_config(TOTP_SECRET)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_one_time_password(
    secret, interval=30, digits=6, sha_version=hashlib.sha1, timepoint=None
):
    key = base64.b32decode(secret, casefold=True)
    time_step = int(timepoint or time.time() / interval)
    msg = struct.pack(">Q", time_step)
    hmac_hash = hmac.new(key, msg, sha_version).digest()
    offset = hmac_hash[-1] & 0x0F
    binary_code = struct.unpack(">I", hmac_hash[offset : offset + 4])[0] & 0x7FFFFFFF
    otp = binary_code % (10**digits)
    return f"{otp:0{digits}d}"


def create_config(totp_secret):
    config = f"""
<clickhouse>
    <profiles>
        <default>
        </default>
    </profiles>
    <users>
        <totuser>
            <password>aa+bb</password>
            <time_based_one_time_password>
                <secret>{totp_secret}</secret>
                <period>60</period>
                <digits>9</digits>
                <algorithm>SHA256</algorithm>
            </time_based_one_time_password>

            <access_management>1</access_management>
            <networks replace="replace">
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </totuser>

        <totuser_no_password>
            <no_password></no_password>
            <time_based_one_time_password>
                <secret>GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ</secret>
            </time_based_one_time_password>
        </totuser_no_password>
    </users>
</clickhouse>
""".lstrip()

    with open(os.path.join(SCRIPT_DIR, "config/users.xml"), "w") as f:
        f.write(config)


def get_totp_for_config(**kwargs):
    return get_one_time_password(
        secret=TOTP_SECRET,
        interval=60,
        digits=9,
        sha_version=hashlib.sha256,
        **kwargs,
    )


def test_one_time_password(started_cluster):
    query_text = "SELECT currentUser() || toString(42)"
    totuser_secret = {
        "secret": TOTP_SECRET,
        "interval": 10,
        "digits": 9,
        "sha_version": hashlib.sha256,
    }

    old_password = get_totp_for_config(
        timepoint=time.time() - 3 * totuser_secret["interval"]
    )
    assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
        query_text, user="totuser", password=f"aa+bb+{old_password}"
    )

    assert "REQUIRED_SECOND_FACTOR" in node.query_and_get_error(
        query_text, user="totuser", password=f"aa+bb"
    )

    assert "totuser42\n" == node.query(
        query_text, user="totuser", password=f"aa+bb+{get_totp_for_config()}"
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
        password=f"aa+bb+{get_totp_for_config()}",
    )
    assert "totuser\tplaintext_password\tone_time_password\tSHA256\t9\t60" in resp


def test_interactive_totp_authentication(started_cluster):
    """Test TOTP authentication in interactive client mode."""
    client_command = f"{started_cluster.get_client_cmd()} --highlight=0 --host {node.ip_address} -u totuser"

    # Password and TOTP provided in command line arguments
    with client(
        command=f"{client_command} --password aa+bb+{get_totp_for_config()}"
    ) as c:
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect("totuser42")
        c.expect(prompt)

    with client(
        command=f"{client_command} --password aa+bb --one-time-password {get_totp_for_config()}"
    ) as c:
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect("totuser42")
        c.expect(prompt)

    # No password provided in command line arguments
    with client(command=f"{client_command}") as c:
        # Enter password + TOTP when prompted
        c.expect("Password.*:")
        c.send(f"aa+bb+{get_totp_for_config()}", eol="\r")
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect("totuser42")
        c.expect(prompt)

    with client(command=f"{client_command}") as c:
        # Enter password when prompted first
        c.expect("Password.*:")
        c.send(f"aa+bb", eol="\r")

        # Then enter TOTP when prompted
        c.expect("TOTP.*:")
        c.send(get_totp_for_config(), eol="\r")
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect("totuser42")
        c.expect(prompt)

    # Password provided in command line arguments, then only TOTP prompted
    with client(command=f"{client_command} --password aa+bb") as c:
        c.expect("TOTP.*:")
        c.send(get_totp_for_config(), eol="\r")
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect("totuser42")
        c.expect(prompt)

    with client(
        command=f"{client_command} --one-time-password {get_totp_for_config()}"
    ) as c:
        # Enter only password, TOTP is provided in command line arguments
        c.expect("Password.*:")
        c.send(f"aa+bb", eol="\r")
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect("totuser42")
        c.expect(prompt)

    # Errors:
    expected_error = re.compile(r"Authentication failed|password is incorrect")

    with client(command=f"{client_command}") as c:
        c.expect("Password.*:")
        c.send(f"aa+bb", eol="\r")

        # Then enter wrong TOTP when prompted
        c.expect("TOTP.*:")
        c.send(f"000000", eol="\r")
        c.expect(expected_error)

    with client(command=f"{client_command} --password aa+bb+000000") as c:
        c.expect(expected_error)

    with client(command=f"{client_command} --password wrongpwd") as c:
        c.expect(expected_error)

    with client(
        command=f"{client_command} --password wrongpwd+{get_totp_for_config()}"
    ) as c:
        c.expect(expected_error)


def test_one_time_only_no_password(started_cluster):
    query_text = "SELECT currentUser() || toString(42)"

    assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
        query_text, user="totuser_no_password", password="000000"
    )

    secret = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
    get_otp = lambda: get_one_time_password(secret=secret)
    assert "totuser_no_password42\n" == node.query(
        query_text, user="totuser_no_password", password=get_otp()
    )

    client_command = f"{started_cluster.get_client_cmd()} --highlight=0 --host {node.ip_address} -u totuser_no_password"
    with client(command=f"{client_command}") as c:
        c.expect("TOTP.*:")
        c.send(get_otp(), eol="\r")
        c.expect(prompt)
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect("totuser_no_password42")
        c.expect(prompt)

    # When main password is empty TOTP works in both places:
    with client(command=f"{client_command} --password {get_otp()}") as c:
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect("totuser_no_password42")
        c.expect(prompt)

    with client(command=f"{client_command} --one-time-password {get_otp()}") as c:
        c.send("SELECT currentUser() || '42' FORMAT TSVRaw;")
        c.expect("totuser_no_password42")
        c.expect(prompt)
