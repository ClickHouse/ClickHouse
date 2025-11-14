#!/bin/bash
set -euxo pipefail

# Generate CA certificate
openssl genrsa -out "${HASHICORP_VAULT_CERT_DIR}/ca.key" 4096
openssl req -x509 -new -nodes -key "${HASHICORP_VAULT_CERT_DIR}/ca.key" -subj "/CN=Vault CA" -out "${HASHICORP_VAULT_CERT_DIR}/ca.crt"

# Generate server certificate
openssl genrsa -out "${HASHICORP_VAULT_CERT_DIR}/server.key" 4096
openssl req -new -key "${HASHICORP_VAULT_CERT_DIR}/server.key" -subj "/CN=hashicorpvault" -out "${HASHICORP_VAULT_CERT_DIR}/server.csr"

cat > "${HASHICORP_VAULT_CERT_DIR}/server-ext.cnf" << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = vault
DNS.3 = hashicorpvault
DNS.4 = 127.0.0.1.nip.io
IP.1 = 127.0.0.1
IP.2 = 0.0.0.0
IP.3 = 172.18.0.2
IP.4 = 172.18.0.1
EOF

openssl x509 -req -in "${HASHICORP_VAULT_CERT_DIR}/server.csr" -CA "${HASHICORP_VAULT_CERT_DIR}/ca.crt" -CAkey "${HASHICORP_VAULT_CERT_DIR}/ca.key" -CAcreateserial -out "${HASHICORP_VAULT_CERT_DIR}/server.crt" -days 365 -sha256 -extfile "${HASHICORP_VAULT_CERT_DIR}/server-ext.cnf"

# Generate client certificate
openssl genrsa -out "${HASHICORP_VAULT_CERT_DIR}/client.key" 4096
openssl req -new -key "${HASHICORP_VAULT_CERT_DIR}/client.key" -subj "/CN=client" -out "${HASHICORP_VAULT_CERT_DIR}/client.csr"
openssl x509 -req -in "${HASHICORP_VAULT_CERT_DIR}/client.csr" -CA "${HASHICORP_VAULT_CERT_DIR}/ca.crt" -CAkey "${HASHICORP_VAULT_CERT_DIR}/ca.key" -CAcreateserial -out "${HASHICORP_VAULT_CERT_DIR}/client.crt" -days 365 -sha256

chmod 644 "${HASHICORP_VAULT_CERT_DIR}/server.crt"
chmod 644 "${HASHICORP_VAULT_CERT_DIR}/server.key"
chmod 644 "${HASHICORP_VAULT_CERT_DIR}/ca.crt"
