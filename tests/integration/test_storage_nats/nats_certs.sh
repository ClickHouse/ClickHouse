#!/usr/bin/env bash
# Generates the TLS material the integration NATS broker needs.
#
# ClickHouseCluster runs this script (from the test directory) when a cluster is started
# with with_nats=True, passing NATS_CERT_DIR in the environment. It must produce:
#   $NATS_CERT_DIR/ca/ca-cert.pem          - CA the Python test client trusts
#   $NATS_CERT_DIR/nats/server-cert.pem    - server certificate (bind-mounted into the broker)
#   $NATS_CERT_DIR/nats/server-key.pem     - server private key
#   $NATS_CERT_DIR/nats/ca-cert.pem        - the CA again, for a broker that verifies clients
#   $NATS_CERT_DIR/client/client-cert.pem  - client certificate signed by the same CA
#   $NATS_CERT_DIR/client/client-key.pem   - client private key
#
# The server certificate is signed by the CA and carries SANs for the broker hostname
# (nats1) and localhost, so the Python client - which verifies the CA and the hostname
# when connecting to tls://localhost - accepts it.
set -e

CERT_DIR="${NATS_CERT_DIR:?NATS_CERT_DIR must be set}"
CA_DIR="$CERT_DIR/ca"
NATS_DIR="$CERT_DIR/nats"
CLIENT_DIR="$CERT_DIR/client"
mkdir -p "$CA_DIR" "$NATS_DIR" "$CLIENT_DIR"

# 1. CA private key + self-signed certificate.
#    OpenSSL 3.x strict verification (used by the Python `ssl` module) requires a CA
#    certificate to carry `basicConstraints=CA:TRUE` together with a `keyUsage` extension
#    that includes `keyCertSign`; otherwise the client rejects the chain with
#    "CA cert does not include key usage extension".
openssl req -newkey rsa:4096 -x509 -days 3650 -nodes -batch \
    -keyout "$CA_DIR/ca-key.pem" -out "$CA_DIR/ca-cert.pem" \
    -subj "/O=ClickHouse/CN=nats-test-ca" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign"

# 2. Server private key + certificate signing request.
openssl req -newkey rsa:4096 -nodes -batch \
    -keyout "$NATS_DIR/server-key.pem" -out "$NATS_DIR/server-req.pem" \
    -subj "/O=ClickHouse/CN=nats1"

# 3. Sign the server CSR with the CA, adding SANs for the broker hostname and localhost
#    plus the key usages a TLS server certificate needs for strict verification.
SAN_CNF="$NATS_DIR/server-ext.cnf"
cat > "$SAN_CNF" <<'EOF'
subjectAltName=DNS:nats1,DNS:localhost,IP:127.0.0.1
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
EOF
openssl x509 -req -days 3650 -in "$NATS_DIR/server-req.pem" \
    -CA "$CA_DIR/ca-cert.pem" -CAkey "$CA_DIR/ca-key.pem" -CAcreateserial \
    -extfile "$SAN_CNF" -out "$NATS_DIR/server-cert.pem"

rm -f "$NATS_DIR/server-req.pem" "$SAN_CNF"

# 4. Client private key + certificate signed by the same CA. A broker started with
#    `--tlsverify` demands it, and one started without ignores it.
openssl req -newkey rsa:4096 -nodes -batch \
    -keyout "$CLIENT_DIR/client-key.pem" -out "$CLIENT_DIR/client-req.pem" \
    -subj "/O=ClickHouse/CN=clickhouse-nats-client"
CLIENT_CNF="$CLIENT_DIR/client-ext.cnf"
cat > "$CLIENT_CNF" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF
openssl x509 -req -days 3650 -in "$CLIENT_DIR/client-req.pem" \
    -CA "$CA_DIR/ca-cert.pem" -CAkey "$CA_DIR/ca-key.pem" -CAcreateserial \
    -extfile "$CLIENT_CNF" -out "$CLIENT_DIR/client-cert.pem"

rm -f "$CLIENT_DIR/client-req.pem" "$CLIENT_CNF"

# 5. Verifying client certificates needs the CA inside the broker's bind-mounted directory.
cp "$CA_DIR/ca-cert.pem" "$NATS_DIR/ca-cert.pem"
