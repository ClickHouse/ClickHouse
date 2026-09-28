import os
import socket
from dataclasses import dataclass

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


# The tests below drive the native protocol over a raw socket instead of going through
# `instance.query`. The regular client can only ask for SSH-key authentication when it holds a
# private key to sign the server's challenge with, so it cannot reach the two cases this test is
# about: a user who authenticates some other way, and a user who does not exist at all. Speaking
# the protocol directly lets us request the challenge for any name and then answer it with
# garbage, which is exactly what an attacker probing for valid user names would do.

# Prefixing the user name with this marker is how a client asks for SSH-key authentication.
# The misspelling is the server's own - see `EncodedUserInfo` in `src/Core/Protocol.h`.
SSH_KEY_AUTHENTICAION_MARKER = " SSH KEY AUTHENTICATION "

# `DBMS_MIN_REVISION_WITH_SSH_AUTHENTICATION` in `src/Core/ProtocolDefines.h`: below this the
# server refuses the SSH handshake outright, so the probe has to claim at least this revision.
REVISION = 54466

# Packet types from `src/Core/Protocol.h`.
CLIENT_HELLO = 0
CLIENT_SSH_CHALLENGE_REQUEST = 11
CLIENT_SSH_CHALLENGE_RESPONSE = 12
SERVER_EXCEPTION = 2
SERVER_SSH_CHALLENGE = 18

AUTHENTICATION_FAILED = 516

# The one failure text every rejected login must share; anything more specific is a disclosure.
GENERIC_FAILURE_TEXT = (
    "Authentication failed: password is incorrect, or there is no user with such name"
)

# The three user states that must be indistinguishable. `default` is deliberately not used as the
# password user: its failure message carries extra password-reset help text of its own.
SSH_USER = "john"  # exists, authenticates with an SSH key
PASSWORD_USER = "paul"  # exists, authenticates with a password
UNKNOWN_USER = "nosuchuser_2f9c41"  # does not exist


def write_varuint(value):
    """Encode an integer the way the native protocol frames all of its lengths and packet types."""
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value & 0x7F)
    return bytes(encoded)


def write_string(value):
    """Encode a string as the protocol's length-prefixed binary string."""
    raw = value.encode() if isinstance(value, str) else bytes(value)
    return write_varuint(len(raw)) + raw


def read_bytes(sock, count):
    """Read exactly `count` bytes, as the protocol's framing is not self-delimiting."""
    received = bytearray()
    while len(received) < count:
        chunk = sock.recv(count - len(received))
        if not chunk:
            raise EOFError(
                f"server closed the connection after {len(received)} of {count} bytes"
            )
        received.extend(chunk)
    return bytes(received)


def read_varuint(sock):
    value = 0
    for byte_index in range(9):
        byte = read_bytes(sock, 1)[0]
        value |= (byte & 0x7F) << (7 * byte_index)
        if not byte & 0x80:
            break
    return value


def read_binary_field(sock):
    """Read a length-prefixed field as raw bytes, for fields that are not text.

    The challenge is a nonce, so it has no encoding to speak of.
    """
    return read_bytes(sock, read_varuint(sock))


def read_string(sock):
    """Read a length-prefixed field that the protocol defines as text."""
    return read_binary_field(sock).decode()


def read_exception(sock):
    """Read a `Server::Exception` packet, whose body has already been identified by the caller."""
    code = int.from_bytes(read_bytes(sock, 4), "little", signed=True)
    read_string(sock)  # exception name, echoed from the code and of no interest here
    message = read_string(sock)
    read_string(sock)  # stack trace
    read_bytes(sock, 1)  # has_nested
    return code, message


@dataclass
class HandshakeResult:
    """What a client holding no credential gets to observe from one SSH handshake attempt.

    `was_challenged` is the interesting part: a server that rejects the login before asking for a
    signature has already told the caller something about the user it was asked about.
    """

    was_challenged: bool
    error_code: int
    error_message: str


def attempt_ssh_handshake(user):
    """Ask for SSH-key authentication as `user` and answer the challenge with an invalid signature.

    The attempt always fails - the point is *how* it fails.
    """
    hello = (
        write_varuint(CLIENT_HELLO)
        + write_string("probe")  # client name
        + write_varuint(24)  # version major
        + write_varuint(3)  # version minor
        + write_varuint(REVISION)
        + write_string("")  # default database
        + write_string(SSH_KEY_AUTHENTICAION_MARKER + user)
        # An empty password is the other half of asking for SSH-key authentication.
        + write_string("")
    )

    sock = socket.create_connection((instance.ip_address, 9000), timeout=20)
    sock.settimeout(20)
    try:
        sock.sendall(hello + write_varuint(CLIENT_SSH_CHALLENGE_REQUEST))

        packet_type = read_varuint(sock)
        if packet_type != SERVER_SSH_CHALLENGE:
            # Rejected without ever asking for a signature. Whatever the error says, the decision
            # itself was made from the user's state.
            assert (
                packet_type == SERVER_EXCEPTION
            ), f"unexpected packet type {packet_type}"
            code, message = read_exception(sock)
            return HandshakeResult(False, code, message)

        read_binary_field(sock)  # the challenge, which we cannot sign

        sock.sendall(
            write_varuint(CLIENT_SSH_CHALLENGE_RESPONSE)
            + write_string("not-a-signature")
        )
        packet_type = read_varuint(sock)
        assert packet_type == SERVER_EXCEPTION, f"unexpected packet type {packet_type}"
        code, message = read_exception(sock)
        return HandshakeResult(True, code, message)
    finally:
        sock.close()


def test_ssh_handshake_does_not_disclose_user_existence():
    """An SSH handshake must look the same whether or not the named user exists.

    All three user states are probed, and every check below runs over all of them at once rather
    than failing on the first, so that a report names every state that is still distinguishable.
    """
    results = {
        user: attempt_ssh_handshake(user)
        for user in (SSH_USER, PASSWORD_USER, UNKNOWN_USER)
    }

    # 1. Every user must be asked for a signature. Answering earlier is itself the disclosure,
    #    regardless of what the error then says.
    rejected_early = {
        user: result.error_message
        for user, result in results.items()
        if not result.was_challenged
    }
    assert not rejected_early, (
        "the handshake answered before asking for a signature, disclosing the user's state: "
        f"{rejected_early}"
    )

    # 2. The failure that follows the bad signature must be the generic one in every case.
    for user, result in results.items():
        assert (
            result.error_code == AUTHENTICATION_FAILED
        ), f"user {user}: expected error {AUTHENTICATION_FAILED}, got {result.error_code}"
        assert (
            GENERIC_FAILURE_TEXT in result.error_message
        ), f"user {user}: unexpected error text {result.error_message}"

    # 3. Nothing may vary between the messages beyond the user's own name, which the client
    #    supplied and therefore already knows.
    messages_without_user_name = {
        result.error_message.replace(user, "<user>") for user, result in results.items()
    }
    assert (
        len(messages_without_user_name) == 1
    ), f"handshake failures are distinguishable: {messages_without_user_name}"
