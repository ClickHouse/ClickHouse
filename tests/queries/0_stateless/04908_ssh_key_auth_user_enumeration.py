#!/usr/bin/env python3
# Tags: no-fasttest
# no-fasttest: the SSH handshake is only compiled into a server built with SSH support.
"""An SSH-key handshake must look the same whether or not the named user exists.

A client asking for SSH-key authentication sends the user name behind a marker and, before proving
anything, receives a challenge to sign. The server used to answer that request only after looking
the user up and checking for an `ssh_key` credential, so a caller holding no key at all could tell
a name that authenticates with an SSH key from a name that authenticates some other way, and both
from a name that does not exist - an oracle for enumerating user names.

The regular client cannot probe this: it asks for SSH-key authentication only when it holds a
private key to sign the challenge with. This test speaks the native protocol over a raw socket
instead, which is what lets it request a challenge for any name and answer it with garbage.
"""

import os
import socket
import subprocess
import sys
from dataclasses import dataclass

HOST = os.environ["CLICKHOUSE_HOST"]
PORT = int(os.environ["CLICKHOUSE_PORT_TCP"])
DATABASE = os.environ["CLICKHOUSE_DATABASE"]

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

# The three user states that must be indistinguishable. The names carry the per-run test database
# so that a test running in parallel neither sees nor collides with them. `default` is deliberately
# not used as the password user: its failure message carries extra password-reset help of its own.
SSH_USER = f"ssh_user_{DATABASE}"
PASSWORD_USER = f"password_user_{DATABASE}"
UNKNOWN_USER = f"nosuchuser_{DATABASE}"

# Any well-formed public key will do: the probe never sends a signature that could match one.
SSH_PUBLIC_KEY_FILE = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "../../config/ssh_user_ed25519_key.pub"
)


def run_query(query):
    """Run a setup query. The port is given explicitly, as the probe below also assumes the
    plain-TCP port rather than letting the client pick between that and the secure one."""
    subprocess.run(
        [
            os.environ["CLICKHOUSE_BINARY"],
            "client",
            "--host",
            HOST,
            "--port",
            str(PORT),
            "--database",
            DATABASE,
            "--query",
            query,
        ],
        check=True,
    )


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
    raw = value.encode()
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


def read_binary_field(sock, as_text=False):
    """Read a length-prefixed field. The challenge is a nonce, so it is not decoded as text."""
    raw = read_bytes(sock, read_varuint(sock))
    return raw.decode() if as_text else raw


def read_exception(sock):
    """Read a `Server::Exception` packet whose type the caller has already read.

    The stack trace and `has_nested` flag that follow the message are left on the socket, which
    this test closes right after.
    """
    code = int.from_bytes(read_bytes(sock, 4), "little", signed=True)
    read_binary_field(sock, as_text=True)  # exception name, echoed from the code
    return code, read_binary_field(sock, as_text=True)


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

    This is the probe an attacker enumerating user names would run: the attempt always fails, so
    the only thing it can learn is *how* it fails.
    """
    hello = (
        write_varuint(CLIENT_HELLO)
        + write_string("probe")  # client name
        # The version pair only lands in the server's `ClientInfo`; the revision below is what
        # gates the handshake, so these two can be anything.
        + write_varuint(24)  # version major
        + write_varuint(3)  # version minor
        + write_varuint(REVISION)
        + write_string("")  # default database
        + write_string(SSH_KEY_AUTHENTICAION_MARKER + user)
        # An empty password is the other half of asking for SSH-key authentication.
        + write_string("")
    )

    sock = socket.create_connection((HOST, PORT), timeout=20)
    sock.settimeout(20)
    try:
        # The hello and the challenge request go out in one write: the server reads the hello and
        # then blocks on the request, so it does not answer in between.
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

        # Holding no key, the probe answers with garbage. That makes the signature check the one
        # thing that rejects it, and the signature check knows nothing about the user's state.
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


def main():
    with open(SSH_PUBLIC_KEY_FILE, encoding="utf-8") as key_file:
        public_key = key_file.read().split()[1]

    run_query(f"DROP USER IF EXISTS {SSH_USER}, {PASSWORD_USER}")
    run_query(
        f"CREATE USER {SSH_USER} IDENTIFIED WITH ssh_key "
        f"BY KEY '{public_key}' TYPE 'ssh-ed25519'"
    )
    run_query(
        f"CREATE USER {PASSWORD_USER} IDENTIFIED WITH sha256_password BY 'password'"
    )

    try:
        results = {
            user: attempt_ssh_handshake(user)
            for user in (SSH_USER, PASSWORD_USER, UNKNOWN_USER)
        }
    finally:
        run_query(f"DROP USER IF EXISTS {SSH_USER}, {PASSWORD_USER}")

    # Every check below runs over all three states rather than failing on the first, so that a
    # failure report names every state that is still distinguishable.
    failures = []

    # 1. Every user must be asked for a signature. Answering earlier is itself the disclosure,
    #    regardless of what the error then says.
    rejected_early = {
        user: result.error_message
        for user, result in results.items()
        if not result.was_challenged
    }
    if rejected_early:
        failures.append(
            "the handshake answered before asking for a signature, disclosing the user's "
            f"state: {rejected_early}"
        )
    else:
        print("every user state was challenged")

    # 2. The failure that follows the bad signature must be the generic one in every case.
    unexpected = {
        user: (result.error_code, result.error_message)
        for user, result in results.items()
        if result.error_code != AUTHENTICATION_FAILED
        or GENERIC_FAILURE_TEXT not in result.error_message
    }
    if unexpected:
        failures.append(f"not the generic authentication failure: {unexpected}")
    else:
        print("every failure is the generic authentication error")

    # 3. Nothing may vary between the messages beyond the user's own name, which the client
    #    supplied and therefore already knows.
    messages_without_user_name = {
        result.error_message.replace(user, "<user>") for user, result in results.items()
    }
    if len(messages_without_user_name) != 1:
        failures.append(f"the failures are distinguishable: {messages_without_user_name}")
    else:
        print("the failures are indistinguishable")

    for failure in failures:
        print(f"FAIL: {failure}")
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
