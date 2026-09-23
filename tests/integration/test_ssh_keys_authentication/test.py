import os
import socket

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    user_configs=["configs/users.xml"],
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_ecdsa():
    assert (
        instance.query(
            "SELECT currentUser()",
            user="john",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/ecdsa",
                "ssh-key-passphrase": "",
            },
        )
        == "john\n"
    )


def test_ed25519():
    assert (
        instance.query(
            "SELECT currentUser()",
            user="john",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/ed25519",
                "ssh-key-passphrase": "",
            },
        )
        == "john\n"
    )


def test_rsa():
    assert (
        instance.query(
            "SELECT currentUser()",
            user="john",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/rsa",
                "ssh-key-passphrase": "",
            },
        )
        == "john\n"
    )


def test_wrong_key():
    with pytest.raises(Exception) as err:
        instance.query(
            "SELECT currentUser()",
            user="john",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/wrong",
                "ssh-key-passphrase": "",
            },
        )

    assert "Authentication failed" in str(err.value)


def test_key_with_passphrase():
    assert (
        instance.query(
            "SELECT currentUser()",
            user="lucy",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/passphrase",
                "ssh-key-passphrase": "passphrase",
            },
        )
        == "lucy\n"
    )


def test_key_with_wrong_passphrase():
    with pytest.raises(Exception):
        instance.query(
            "SELECT currentUser()",
            user="lucy",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/passphrase",
                "ssh-key-passphrase": "wrong",
            },
        ) == "lucy\n"


# The native protocol lets a client ask for SSH-key authentication by prefixing the user name in
# the Hello with this marker. The handshake that follows must not tell an unauthenticated client
# whether the named user exists, or how it authenticates, before a signature has been checked.
SSH_KEY_AUTHENTICAION_MARKER = " SSH KEY AUTHENTICATION "

# Any revision at or above DBMS_MIN_REVISION_WITH_SSH_AUTHENTICATION; the handshake never gets as
# far as the server Hello, so no later protocol feature is negotiated.
REVISION = 54466

CLIENT_HELLO = 0
CLIENT_SSH_CHALLENGE_REQUEST = 11
CLIENT_SSH_CHALLENGE_RESPONSE = 12
SERVER_EXCEPTION = 2
SERVER_SSH_CHALLENGE = 18

AUTHENTICATION_FAILED = 516
UNIFIED_FAILURE = (
    "Authentication failed: password is incorrect, or there is no user with such name"
)

# The names of the three user states the handshake must not distinguish. `default` is avoided
# deliberately: its failure message carries an extra password-reset help text.
SSH_USER = "john"
PASSWORD_USER = "paul"
UNKNOWN_USER = "nosuchuser_2f9c41"


def varuint(n):
    buf = bytearray()
    while n >= 0x80:
        buf.append((n & 0x7F) | 0x80)
        n >>= 7
    buf.append(n & 0x7F)
    return bytes(buf)


def varstring(s):
    b = s.encode() if isinstance(s, str) else bytes(s)
    return varuint(len(b)) + b


def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError()
        buf.extend(chunk)
    return bytes(buf)


def read_varuint(sock):
    x = 0
    for i in range(9):
        b = recv_exact(sock, 1)[0]
        x |= (b & 0x7F) << (7 * i)
        if not (b & 0x80):
            return x
    return x


def read_varstring(sock):
    return recv_exact(sock, read_varuint(sock))


def ssh_handshake(user):
    """Ask for SSH-key authentication as `user` and answer the challenge with a signature that
    cannot be valid. Returns what an unauthenticated client can observe: the type of the first
    packet the server sends, then the error code and text it fails with."""
    hello = (
        varuint(CLIENT_HELLO)
        + varstring("probe")  # client name
        + varuint(24)  # version major
        + varuint(3)  # version minor
        + varuint(REVISION)
        + varstring("")  # default database
        + varstring(SSH_KEY_AUTHENTICAION_MARKER + user)
        + varstring("")  # password (empty -> SSH-key authentication is requested)
    )

    sock = socket.create_connection((instance.ip_address, 9000), timeout=20)
    sock.settimeout(20)
    try:
        sock.sendall(hello + varuint(CLIENT_SSH_CHALLENGE_REQUEST))

        first_packet = read_varuint(sock)
        if first_packet != SERVER_SSH_CHALLENGE:
            code, _name, message = read_exception(sock)
            return first_packet, code, message
        read_varstring(sock)  # challenge

        sock.sendall(
            varuint(CLIENT_SSH_CHALLENGE_RESPONSE) + varstring("not-a-signature")
        )
        assert read_varuint(sock) == SERVER_EXCEPTION
        code, _name, message = read_exception(sock)
        return first_packet, code, message
    finally:
        sock.close()


def read_exception(sock):
    code = int.from_bytes(recv_exact(sock, 4), "little", signed=True)
    name = read_varstring(sock).decode()
    message = read_varstring(sock).decode()
    read_varstring(sock)  # stack trace
    recv_exact(sock, 1)  # has_nested
    return code, name, message


def test_ssh_handshake_does_not_disclose_user_existence():
    """The three user states — SSH user, user authenticating some other way, and a user that does
    not exist — must be indistinguishable to a client that holds no credential: each is challenged
    and each then fails identically. Only the SSH user used to be challenged; the other two were
    turned away before any signature was asked for, with `Expected authentication with SSH key`
    and `There is no user ...` respectively, which made the handshake a user-existence oracle."""
    observed = {
        user: ssh_handshake(user)
        for user in (SSH_USER, PASSWORD_USER, UNKNOWN_USER)
    }

    # Collected rather than asserted per user, so that a server which distinguishes the states
    # reports every one of them at once.
    answered_early = {
        user: message
        for user, (first_packet, _code, message) in observed.items()
        if first_packet != SERVER_SSH_CHALLENGE
    }
    assert not answered_early, (
        "the handshake answered before asking for a signature, disclosing the user's state: "
        f"{answered_early}"
    )

    for user, (_first_packet, code, message) in observed.items():
        assert code == AUTHENTICATION_FAILED, f"user {user}: unexpected error code {code}"
        assert UNIFIED_FAILURE in message, f"user {user}: unexpected error text {message}"

    # The message echoes the name the client sent, which the client already knows; nothing else in
    # it may vary between the three states.
    distinct = {
        message.replace(user, "<user>")
        for user, (_first_packet, _code, message) in observed.items()
    }
    assert len(distinct) == 1, f"handshake failures are distinguishable: {distinct}"
