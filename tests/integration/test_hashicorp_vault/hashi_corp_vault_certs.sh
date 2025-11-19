#!/bin/bash
set -euxo pipefail

# Generate CA certificate
openssl genrsa -out "${HASHICORP_VAULT_CERT_DIR}/ca.key" 4096
openssl req -x509 -new -nodes -key "${HASHICORP_VAULT_CERT_DIR}/ca.key" -subj "/CN=Vault CA" -out "${HASHICORP_VAULT_CERT_DIR}/ca.crt"

# Generate server certificate
openssl genrsa -out "${HASHICORP_VAULT_CERT_DIR}/server.key" 4096
openssl req -new -key "${HASHICORP_VAULT_CERT_DIR}/server.key" -subj "/CN=hashicorpvault" -out "${HASHICORP_VAULT_CERT_DIR}/server.csr"
openssl x509 -req -in "${HASHICORP_VAULT_CERT_DIR}/server.csr" -CA "${HASHICORP_VAULT_CERT_DIR}/ca.crt" -CAkey "${HASHICORP_VAULT_CERT_DIR}/ca.key" -CAcreateserial -out "${HASHICORP_VAULT_CERT_DIR}/server.crt" -days 365 -sha256

# Generate client certificate
openssl genrsa -out "${HASHICORP_VAULT_CERT_DIR}/client.key" 4096
openssl req -new -key "${HASHICORP_VAULT_CERT_DIR}/client.key" -subj "/CN=client" -out "${HASHICORP_VAULT_CERT_DIR}/client.csr"
openssl x509 -req -in "${HASHICORP_VAULT_CERT_DIR}/client.csr" -CA "${HASHICORP_VAULT_CERT_DIR}/ca.crt" -CAkey "${HASHICORP_VAULT_CERT_DIR}/ca.key" -CAcreateserial -out "${HASHICORP_VAULT_CERT_DIR}/client.crt" -days 365 -sha256

chmod 644 "${HASHICORP_VAULT_CERT_DIR}/client.crt"
chmod 644 "${HASHICORP_VAULT_CERT_DIR}/server.crt"
chmod 644 "${HASHICORP_VAULT_CERT_DIR}/server.key"
chmod 644 "${HASHICORP_VAULT_CERT_DIR}/ca.crt"
