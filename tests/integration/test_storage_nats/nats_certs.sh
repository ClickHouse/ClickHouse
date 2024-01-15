#!/bin/bash
set -euxo pipefail

mkdir -p "${NATS_CERT_DIR}/ca"
mkdir -p "${NATS_CERT_DIR}/nats"
openssl req -newkey rsa:4096 -x509 -days 365 -nodes -batch -keyout "${NATS_CERT_DIR}/ca/ca-key.pem" -out "${NATS_CERT_DIR}/ca/ca-cert.pem" -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=ca"
openssl req -newkey rsa:4096 -nodes -batch -keyout "${NATS_CERT_DIR}/nats/server-key.pem" -out "${NATS_CERT_DIR}/nats/server-req.pem" -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=server"
openssl x509 -req -days 365 -in "${NATS_CERT_DIR}/nats/server-req.pem" -CA "${NATS_CERT_DIR}/ca/ca-cert.pem" -CAkey "${NATS_CERT_DIR}/ca/ca-key.pem" -CAcreateserial -out "${NATS_CERT_DIR}/nats/server-cert.pem" -extfile <(
cat <<-EOF
subjectAltName = DNS:localhost, DNS:nats1
EOF
)
rm -f "${NATS_CERT_DIR}/nats/server-req.pem"
