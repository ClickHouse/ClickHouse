#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the PostgreSQL protocol port

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A SCRAM-SHA-256 user makes the server initiate SASL authentication, so the SASL
# message readers are reachable before the client authenticates. Bind and CopyData
# are reached after authentication, so the probe completes a full SCRAM handshake.
USER="pgscram_${CLICKHOUSE_DATABASE}"
TABLE="pgcopy_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${USER} HOST IP '127.0.0.1' IDENTIFIED WITH scram_sha256_password BY 'pass'"
$CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS ${TABLE} (x Int32) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "GRANT INSERT ON ${TABLE} TO ${USER}"

# Feed each message reader a wire-supplied length that is negative or below 4, which
# previously underflowed into resize/reserve/string-construction. The server must
# answer every malformed frame with an ErrorResponse ('E') and stay alive; before the
# fix a sanitizer/debug build aborted instead.
python3 - "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_POSTGRESQL" "$USER" "pass" "$CLICKHOUSE_DATABASE" "$TABLE" <<'PY'
import socket, struct, sys, hashlib, hmac, base64

host, port, user, password, database, table = (
    sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])


def recv_msg(s):
    t = s.recv(1)
    if not t:
        return None, None
    length = struct.unpack(">I", s.recv(4))[0]
    body = b""
    while len(body) < length - 4:
        body += s.recv(length - 4 - len(body))
    return t, body


def startup(s):
    params = b"user\x00" + user.encode() + b"\x00database\x00" + database.encode() + b"\x00\x00"
    s.sendall(struct.pack(">I", len(params) + 8) + struct.pack(">I", 196608) + params)
    return recv_msg(s)  # AuthenticationSASL ('R', 10)


def scram_authenticate(s):
    startup(s)
    client_nonce = "clientnonceabcdef"
    client_first_bare = "n=,r=" + client_nonce
    client_first = "n,," + client_first_bare
    msg = b"SCRAM-SHA-256\x00" + struct.pack(">i", len(client_first)) + client_first.encode()
    s.sendall(b"p" + struct.pack(">I", len(msg) + 4) + msg)

    _, body = recv_msg(s)  # AuthenticationSASLContinue + server-first-message
    server_first = body[4:].decode()
    attrs = dict(kv.split("=", 1) for kv in server_first.split(","))
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), base64.b64decode(attrs["s"]), int(attrs["i"]))
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    client_final_no_proof = "c=biws,r=" + attrs["r"]
    auth_message = client_first_bare + "," + server_first + "," + client_final_no_proof
    client_sig = hmac.new(stored_key, auth_message.encode(), hashlib.sha256).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, client_sig))
    client_final = client_final_no_proof + ",p=" + base64.b64encode(proof).decode()
    s.sendall(b"p" + struct.pack(">I", len(client_final) + 4) + client_final.encode())

    while True:  # AuthenticationSASLFinal, AuthenticationOk, ParameterStatus*, ReadyForQuery
        t, body = recv_msg(s)
        if t is None or t == b"E":
            raise RuntimeError("authentication failed")
        if t == b"Z":
            return


def reports_error(s):
    try:
        t, _ = recv_msg(s)
    except Exception:
        return False
    return t == b"E"


def connect():
    s = socket.create_connection((host, port), timeout=10)
    s.settimeout(10)
    return s


# Malformed SASLInitialResponse: negative SASL mechanism length -> resize underflow.
s = connect()
startup(s)
body = b"SCRAM-SHA-256\x00" + struct.pack(">i", -1)
s.sendall(b"p" + struct.pack(">I", len(body) + 4) + body)
print("SASLInitialResponse rejected:", reports_error(s))
s.close()

# Malformed SASLResponse: message length below 4 -> (size - 4) underflow.
s = connect()
startup(s)
cfirst = b"n,,n=,r=clientnonceabcdef"
body = b"SCRAM-SHA-256\x00" + struct.pack(">i", len(cfirst)) + cfirst
s.sendall(b"p" + struct.pack(">I", len(body) + 4) + body)  # valid SASLInitialResponse
recv_msg(s)  # AuthenticationSASLContinue
s.sendall(b"p" + struct.pack(">I", 0))  # SASLResponse with size = 0
print("SASLResponse rejected:", reports_error(s))
s.close()

# Malformed Bind: parameter length -1 -> std::string(size_t(-1), 0) underflow.
s = connect()
scram_authenticate(s)
body = b"\x00" + b"\x00" + struct.pack(">h", 0) + struct.pack(">h", 1) + struct.pack(">i", -1)
s.sendall(b"B" + struct.pack(">I", len(body) + 4) + body)
print("Bind rejected:", reports_error(s))
s.close()

# Malformed CopyData: message length below 4 -> (size - sizeof(Int32)) underflow.
s = connect()
scram_authenticate(s)
query = ("COPY " + table + " FROM STDIN WITH FORMAT csv\x00").encode()
s.sendall(b"Q" + struct.pack(">I", len(query) + 4) + query)
recv_msg(s)  # CopyInResponse ('G')
s.sendall(b"d" + struct.pack(">I", 0))  # CopyData with size = 0
print("CopyData rejected:", reports_error(s))
s.close()
PY

# The server is still alive after the malformed frames.
$CLICKHOUSE_CLIENT --query "SELECT 1"

$CLICKHOUSE_CLIENT --query "DROP TABLE ${TABLE}"
$CLICKHOUSE_CLIENT --query "DROP USER ${USER}"
