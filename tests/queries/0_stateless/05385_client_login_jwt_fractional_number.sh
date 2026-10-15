#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the client in the fast test build is compiled without OAuth support, so --login does nothing there.

# `clickhouse client --login` decodes the access token returned by the identity provider.
# A fractional number in its payload (RFC 7519 allows one in `iat`) must not abort the client.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$DIR/bin"
printf '#!/bin/sh\nexit 0\n' > "$DIR/bin/xdg-open"
chmod +x "$DIR/bin/xdg-open"

openssl req -newkey rsa:2048 -nodes -keyout "$DIR/key.pem" -x509 -days 1 -out "$DIR/cert.pem" \
    -subj "/CN=localhost" >/dev/null 2>&1

cat > "$DIR/idp.py" <<'EOF'
import base64, http.server, json, os, ssl, sys

d = sys.argv[1]
b64 = lambda o: base64.urlsafe_b64encode(json.dumps(o).encode()).rstrip(b"=").decode()
# No `expires_in` in the token response, so the client takes the expiry from the token itself.
token = b64({"alg": "none", "typ": "JWT"}) + "." + b64({"sub": "test", "aud": "test", "iss": "test", "exp": 4102444800, "iat": 1700000000.5}) + ".c2ln"

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path == "/device":
            body = {"device_code": "dc", "user_code": "UC", "verification_uri": "https://localhost/verify", "expires_in": 60, "interval": 1}
        else:
            body = {"access_token": token, "token_type": "Bearer"}
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass

server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(d + "/cert.pem", d + "/key.pem")
server.socket = context.wrap_socket(server.socket, server_side=True)
with open(d + "/port.tmp", "w") as f:
    f.write(str(server.server_address[1]))
os.rename(d + "/port.tmp", d + "/port")
server.serve_forever()
EOF

python3 "$DIR/idp.py" "$DIR" &
IDP_PID=$!
trap '{ kill $IDP_PID; wait $IDP_PID; } 2>/dev/null; rm -rf "$DIR"' EXIT

for _ in {1..100}; do
    [ -f "$DIR/port" ] && break
    sleep 0.1
done
PORT=$(cat "$DIR/port")

# A client that decoded the token sends it to the server, which rejects it: a server without JWT authentication
# does not know the user (AUTHENTICATION_FAILED), a server with JWT authentication refuses the unsigned token
# (AUTHENTICATION_FAILED or BAD_ARGUMENTS). A client that died while decoding the token prints neither.
PATH="$DIR/bin:$PATH" "$CLICKHOUSE_CLIENT_BINARY" --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT_TCP" \
    --login --oauth-client-id test \
    --oauth-device-uri "https://127.0.0.1:$PORT/device" --oauth-token-uri "https://127.0.0.1:$PORT/token" \
    --accept-invalid-certificate -q "SELECT 1" 2>&1 | grep -q -E "AUTHENTICATION_FAILED|BAD_ARGUMENTS" && echo "rejected by the server"
