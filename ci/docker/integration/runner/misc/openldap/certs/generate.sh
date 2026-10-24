#!/bin/bash
# Regenerates the self-signed CA and the slapd server certificate used by the
# `openldap` integration-test fixture (tests/integration/compose/docker_compose_ldap.yml).
#
# The certificate is valid for 100 years and carries the SANs that the tests connect
# with (`openldap` from other containers, `localhost` / 127.0.0.1 from inside the
# container). The private key is checked in with mode 0644 on purpose: the bitnami
# image runs slapd as uid 1001 and reads the key from a read-only bind mount.
#
# Only `ca.pem`, `server.pem` and `server-key.pem` are consumed; the CA key is
# discarded because nothing else is ever signed by this CA.
set -euo pipefail

cd "$(dirname "$0")"

DAYS=36500
SAN="DNS:openldap,DNS:localhost,IP:127.0.0.1"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

openssl ecparam -name prime256v1 -genkey -noout -out "$tmp/ca-key.pem"
openssl req -x509 -new -key "$tmp/ca-key.pem" -sha256 -days "$DAYS" \
    -subj "/CN=ClickHouse integration tests LDAP CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out ca.pem

openssl ecparam -name prime256v1 -genkey -noout -out server-key.pem
openssl req -new -key server-key.pem -sha256 \
    -subj "/CN=openldap" \
    -out "$tmp/server.csr"

cat > "$tmp/server-ext.cnf" <<EXT
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment,keyAgreement
extendedKeyUsage=serverAuth
subjectAltName=$SAN
EXT

openssl x509 -req -in "$tmp/server.csr" -CA ca.pem -CAkey "$tmp/ca-key.pem" \
    -CAcreateserial -sha256 -days "$DAYS" -extfile "$tmp/server-ext.cnf" \
    -out server.pem
rm -f ca.srl

chmod 0644 ca.pem server.pem server-key.pem

openssl verify -CAfile ca.pem server.pem
openssl x509 -in server.pem -noout -subject -ext subjectAltName -enddate
