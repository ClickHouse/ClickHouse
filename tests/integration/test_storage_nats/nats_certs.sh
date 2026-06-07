#!/usr/bin/env bash
# Generates the TLS material the integration NATS broker needs.
#
# ClickHouseCluster runs this script (from the test directory) when a cluster is started
# with with_nats=True, passing NATS_CERT_DIR in the environment. It must produce:
#   $NATS_CERT_DIR/ca/ca-cert.pem        - CA the Python test client trusts
#   $NATS_CERT_DIR/nats/server-cert.pem  - server certificate (bind-mounted into the broker)
#   $NATS_CERT_DIR/nats/server-key.pem   - server private key
#
# The server certificate is signed by the CA and carries SANs for the broker hostname
# (nats1) and localhost, so the Python client - which verifies the CA and the hostname
# when connecting to tls://localhost - accepts it.
set -e

CERT_DIR="${NATS_CERT_DIR:?NATS_CERT_DIR must be set}"
CA_DIR="$CERT_DIR/ca"
NATS_DIR="$CERT_DIR/nats"
mkdir -p "$CA_DIR" "$NATS_DIR"

# 1. CA private key + self-signed certificate.
openssl req -newkey rsa:4096 -x509 -days 3650 -nodes -batch \
    -keyout "$CA_DIR/ca-key.pem" -out "$CA_DIR/ca-cert.pem" \
    -subj "/O=ClickHouse/CN=nats-test-ca"

# 2. Server private key + certificate signing request.
openssl req -newkey rsa:4096 -nodes -batch \
    -keyout "$NATS_DIR/server-key.pem" -out "$NATS_DIR/server-req.pem" \
    -subj "/O=ClickHouse/CN=nats1"

# 3. Sign the server CSR with the CA, adding SANs for the broker hostname and localhost.
SAN_CNF="$NATS_DIR/server-ext.cnf"
printf "subjectAltName=DNS:nats1,DNS:localhost,IP:127.0.0.1\n" > "$SAN_CNF"
openssl x509 -req -days 3650 -in "$NATS_DIR/server-req.pem" \
    -CA "$CA_DIR/ca-cert.pem" -CAkey "$CA_DIR/ca-key.pem" -CAcreateserial \
    -extfile "$SAN_CNF" -out "$NATS_DIR/server-cert.pem"

rm -f "$NATS_DIR/server-req.pem" "$SAN_CNF"
