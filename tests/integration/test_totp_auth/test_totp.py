import pytest


import base64
import hmac
import struct
import time
import hashlib
from fnmatch import fnmatch


from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["config/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def generate_totp(secret, interval=30, digits=6):
    key = base64.b32decode(secret, casefold=True)
    time_step = int(time.time() / interval)
    msg = struct.pack(">Q", time_step)
    hmac_hash = hmac.new(key, msg, hashlib.sha1).digest()
    offset = hmac_hash[-1] & 0x0F
    binary_code = struct.unpack(">I", hmac_hash[offset : offset + 4])[0] & 0x7FFFFFFF
    otp = binary_code % (10**digits)
    return f"{otp:0{digits}d}"


def test_one_time_password(started_cluster):
    query_text = "SELECT currentUser() || toString(42)"

    totuser_secret = {"secret": "INWGSY3LJBXXK43FBIAA====", "interval": 10, "digits": 9}
    old_password = generate_totp(**totuser_secret)
    old_password_created = time.time()
    assert "totuser42\n" == node.query(
        query_text, user="totuser", password=old_password
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

    node.query(
        "CREATE USER user2 IDENTIFIED WITH one_time_password BY 'WLFVSILVT3PKZPKONCMGAHN7KBPTUX2J'",
        user="totuser",
        password=generate_totp(**totuser_secret),
    )

    assert "user242\n" == node.query(
        query_text,
        user="user2",
        password=generate_totp("WLFVSILVT3PKZPKONCMGAHN7KBPTUX2J"),
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
    assert resp[0].startswith("totuser\tone_time_password\tSHA1\t9\t10"), resp
    assert resp[1].startswith("user2\tone_time_password"), resp

    # check that old password invalidated
    elapsed = int(time.time() - old_password_created)
    for _ in range(20 - elapsed):
        time.sleep(1)
        print(".", end="", flush=True)
    print()

    assert "AUTHENTICATION_FAILED" in node.query_and_get_error(
        query_text, user="totuser", password=old_password
    )
