#!/bin/bash
# Generate TLS certificates for the NATS integration test.
# cluster.py calls this script with $NATS_CERT_DIR set to the target directory.
# Expected layout after this script:
#   $NATS_CERT_DIR/ca/ca-cert.pem   (CA cert, loaded by Python ssl.create_default_context)
#   $NATS_CERT_DIR/ca/ca-key.pem    (CA key)
#   $NATS_CERT_DIR/nats/server-cert.pem  (server cert, mounted into NATS container)
#   $NATS_CERT_DIR/nats/server-key.pem   (server key, mounted into NATS container)

set -e

CA_DIR="${NATS_CERT_DIR}/ca"
NATS_DIR="${NATS_CERT_DIR}/nats"

mkdir -p "${CA_DIR}" "${NATS_DIR}"

# 1. Generate CA private key and self-signed certificate
openssl req -newkey rsa:2048 -x509 -days 3650 -nodes -batch \
    -keyout "${CA_DIR}/ca-key.pem" \
    -out    "${CA_DIR}/ca-cert.pem" \
    -subj "/CN=nats-test-ca"

# 2. Generate server private key and CSR
openssl req -newkey rsa:2048 -nodes -batch \
    -keyout "${NATS_DIR}/server-key.pem" \
    -out    "${NATS_DIR}/server-req.pem" \
    -subj "/CN=nats1"

# 3. Sign the server cert with the CA
openssl x509 -req -days 3650 \
    -in  "${NATS_DIR}/server-req.pem" \
    -CA  "${CA_DIR}/ca-cert.pem" \
    -CAkey "${CA_DIR}/ca-key.pem" \
    -CAcreateserial \
    -out "${NATS_DIR}/server-cert.pem"

rm -f "${NATS_DIR}/server-req.pem"
