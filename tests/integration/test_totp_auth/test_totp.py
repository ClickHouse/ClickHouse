import base64
import hashlib
import hmac
import os
import random
import struct
import time
from fnmatch import fnmatch

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TOTP_SECRET = base64.b32encode(random.randbytes(random.randint(3, 64))).decode()

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["config/users.xml"],
)


def create_config(totp_secret):
    config = f"""
<clickhouse>
    <profiles>
        <default>
        </default>
    </profiles>
    <users>
        <totuser>
            <time_based_one_time_password>
                <secret>{totp_secret}</secret>
                <period>10</period>
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
    </users>
</clickhouse>
""".lstrip()

    with open(os.path.join(SCRIPT_DIR, "config/users.xml"), "w") as f:
        f.write(config)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        create_config(TOTP_SECRET)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def generate_totp(
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


def test_one_time_password(started_cluster):
    query_text = "SELECT currentUser() || toString(42)"
    totuser_secret = {
        "secret": TOTP_SECRET,
        "interval": 10,
        "digits": 9,
        "sha_version": hashlib.sha256,
    }

    old_password = generate_totp(
        **totuser_secret, timepoint=time.time() - 3 * totuser_secret["interval"]
    )
    assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
        query_text, user="totuser", password=old_password
    )

    assert "totuser42\n" == node.query(
        query_text, user="totuser", password=generate_totp(**totuser_secret)
    )

    assert "totuser42\n" == node.query(
        query_text, user="totuser", password=generate_totp(**totuser_secret)
    )

    assert "CREATE USER totuser IDENTIFIED WITH one_time_password" in node.query(
        "SHOW CREATE USER totuser",
        user="totuser",
        password=generate_totp(**totuser_secret),
    )

    for bad_secret, error_message in [
        ("i11egalbase32", "Invalid character in*secret"),
        ("abc$d", "Invalid character in*secret"),
        ("   ", "Empty secret"),
        ("   =", "Empty secret"),
        ("", "Empty secret"),
    ]:
        err_resp = node.query_and_get_error(
            f"CREATE USER user2 IDENTIFIED WITH one_time_password BY '{bad_secret}'",
            user="totuser",
            password=generate_totp(**totuser_secret),
        )
        assert fnmatch(err_resp, f"*{error_message}*BAD_ARGUMENTS*"), err_resp

    # lowercase secret with spaces is allowed
    node.query(
        "CREATE USER user2 IDENTIFIED WITH one_time_password BY 'inwg sy3l jbxx k43f biaa'",
        user="totuser",
        password=generate_totp(**totuser_secret),
    )

    assert "user242\n" == node.query(
        query_text,
        user="user2",
        password=generate_totp("INWGSY3LJBXXK43FBIAA===="),
    )

    resp = node.query(
        """
            SELECT
                name,
                auth_type[1],
                JSONExtractString(auth_params[1], 'algorithm'),
                JSONExtractString(auth_params[1], 'num_digits'),
                JSONExtractString(auth_params[1], 'period')
            FROM system.users WHERE name IN ('totuser', 'user2')
            ORDER BY 1
            """,
        user="totuser",
        password=generate_totp(**totuser_secret),
    ).splitlines()
    assert resp[0].startswith("totuser\tone_time_password\tSHA256\t9\t10"), resp
    assert resp[1].startswith("user2\tone_time_password"), resp
