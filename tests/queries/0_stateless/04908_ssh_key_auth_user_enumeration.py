#!/usr/bin/env python3
# Tags: no-fasttest, no-openssl-fips
# no-fasttest: the SSH handshake is only compiled into a server built with SSH support.
# no-openssl-fips: creates an ssh-ed25519 credential, which FIPS builds reject.
"""Test that the SSH-key handshake does not reveal whether a user exists.

A client asks for SSH-key authentication by prefixing the user name with a marker, and the server
answers with a challenge to sign. The server used to look the user up and check for an `ssh_key`
credential before sending that challenge, so the three user states answered differently: a user
with an SSH key got the challenge, a user with a password got "Expected authentication with SSH
key", and an unknown name got "There is no user ...". Telling them apart needed no key at all,
which made the handshake a way to enumerate user names.

The client asks for SSH-key authentication only when it holds a private key to sign with, so it
cannot reach the other two states. This test speaks the native protocol over a raw socket
instead: it requests a challenge for each name and answers with garbage. All three attempts fail,
and they must fail the same way.

https://github.com/ClickHouse/ClickHouse/pull/121704
"""

import os
import socket
import subprocess

HOST = os.environ["CLICKHOUSE_HOST"]
PORT = int(os.environ["CLICKHOUSE_PORT_TCP"])
DATABASE = os.environ["CLICKHOUSE_DATABASE"]
CLIENT = [
    os.environ["CLICKHOUSE_BINARY"],
    "client",
    "--host",
    HOST,
    "--port",
    str(PORT),
    "--database",
    DATABASE,
]

TIMEOUT = 20

# Packet types, from src/Core/Protocol.h.
CLIENT_HELLO = 0
CLIENT_SSH_CHALLENGE_REQUEST = 11
CLIENT_SSH_CHALLENGE_RESPONSE = 12
SERVER_EXCEPTION = 2
SERVER_SSH_CHALLENGE = 18

# A user name behind this marker is a request for SSH-key authentication. The misspelling is the
# server's own, see `EncodedUserInfo` in src/Core/Protocol.h.
SSH_KEY_MARKER = " SSH KEY AUTHENTICATION "

# The server refuses the SSH handshake below `DBMS_MIN_REVISION_WITH_SSH_AUTHENTICATION`
# (src/Core/ProtocolDefines.h), so claim at least that revision.
REVISION = 54466

AUTHENTICATION_FAILED = 516
GENERIC_ERROR = (
    "Authentication failed: password is incorrect, or there is no user with such name"
)

# The three user states that must be indistinguishable. The test database is unique per run, so
# naming the users after it keeps parallel runs apart. The password user is not `default`, whose
# error message carries extra password-reset help.
SSH_USER = f"ssh_user_{DATABASE}"
PASSWORD_USER = f"password_user_{DATABASE}"
UNKNOWN_USER = f"nosuchuser_{DATABASE}"

# Any valid public key will do, because no signature the test sends can match it.
PUBLIC_KEY_FILE = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "../../config/ssh_user_ed25519_key.pub"
)


def run_query(query):
    subprocess.run(CLIENT + ["--query", query], check=True)


def write_varuint(value):
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def write_string(value):
    data = value.encode()
    return write_varuint(len(data)) + data


def read_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError(f"connection closed after {len(data)} of {size} bytes")
        data += chunk
    return data


def read_varuint(sock):
    value = 0
    for i in range(9):
        byte = read_exact(sock, 1)[0]
        value |= (byte & 0x7F) << (7 * i)
        if not byte & 0x80:
            break
    return value


def read_string(sock):
    return read_exact(sock, read_varuint(sock)).decode()


def read_exception(sock):
    # Code, name and message. The stack trace and the nested flag that follow are not read,
    # because the connection is closed right after.
    code = int.from_bytes(read_exact(sock, 4), "little", signed=True)
    read_string(sock)
    message = read_string(sock)
    return code, message


def ssh_handshake(user):
    """Request SSH-key authentication as `user` and answer the challenge with a bad signature.

    Returns whether the server asked for a signature at all, and the error it ended with.
    """
    hello = (
        write_varuint(CLIENT_HELLO)
        + write_string("probe")  # client name
        + write_varuint(24)  # client version major
        + write_varuint(3)  # client version minor
        + write_varuint(REVISION)
        + write_string("")  # default database
        + write_string(SSH_KEY_MARKER + user)
        + write_string("")  # an empty password completes the SSH-key request
    )

    with socket.create_connection((HOST, PORT), timeout=TIMEOUT) as sock:
        sock.sendall(hello + write_varuint(CLIENT_SSH_CHALLENGE_REQUEST))  # and ask for a challenge

        packet = read_varuint(sock)
        if packet == SERVER_EXCEPTION:
            code, message = read_exception(sock)
            return False, code, message
        assert packet == SERVER_SSH_CHALLENGE, f"unexpected packet {packet}"

        read_exact(sock, read_varuint(sock))  # the challenge, which the test cannot sign
        sock.sendall(
            write_varuint(CLIENT_SSH_CHALLENGE_RESPONSE) + write_string("not-a-signature")
        )

        packet = read_varuint(sock)
        assert packet == SERVER_EXCEPTION, f"unexpected packet {packet}"
        code, message = read_exception(sock)
        return True, code, message


def main():
    with open(PUBLIC_KEY_FILE, encoding="utf-8") as key_file:
        public_key = key_file.read().split()[1]

    run_query(f"DROP USER IF EXISTS {SSH_USER}, {PASSWORD_USER}")
    try:
        run_query(
            f"CREATE USER {SSH_USER} IDENTIFIED WITH ssh_key BY KEY '{public_key}' TYPE 'ssh-ed25519'"
        )
        run_query(f"CREATE USER {PASSWORD_USER} IDENTIFIED WITH sha256_password BY 'password'")

        errors = set()
        for user in (SSH_USER, PASSWORD_USER, UNKNOWN_USER):
            challenged, code, message = ssh_handshake(user)
            # Being refused before the challenge is itself a disclosure, whatever the error says.
            assert challenged, f"{user} was refused before the challenge: {message}"
            assert code == AUTHENTICATION_FAILED, f"{user}: error {code}: {message}"
            assert GENERIC_ERROR in message, f"{user}: unexpected error: {message}"
            # The name is the one thing the caller already knows, so it may differ - nothing else.
            errors.add(message.replace(user, "USER"))
    finally:
        run_query(f"DROP USER IF EXISTS {SSH_USER}, {PASSWORD_USER}")

    print("every user was challenged and then got the generic authentication error")
    assert len(errors) == 1, f"the errors differ: {errors}"
    print("the errors are identical apart from the user name")


if __name__ == "__main__":
    main()
