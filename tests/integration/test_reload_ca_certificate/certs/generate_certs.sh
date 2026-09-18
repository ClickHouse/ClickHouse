#!/bin/bash
# Generates the certificates for test_reload_ca_certificate:
#   ca1.crt / ca1.key   - first root CA
#   ca2.crt / ca2.key   - second, unrelated root CA
#   cert1.crt / cert1.key - leaf certificate issued by ca1
#   cert2.crt / cert2.key - leaf certificate issued by ca2
# Both leaf certificates are valid for all host names used in the test and carry no extended key usage,
# so they can be used as server, client and Keeper (Raft) certificates.
#   ca.crt, node.crt, node.key - copies of ca1.crt, cert1.crt, cert1.key: the initial content of the files
#   that the configs point to and that the test overwrites to rotate certificates.
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"

DAYS=3650

cat > leaf.cnf << 'EOC'
[req]
distinguished_name = dn
prompt = no
[dn]
CN = clickhouse-test
[v3_leaf]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
subjectAltName = DNS:localhost, DNS:node, DNS:node1, DNS:node2, DNS:node3, IP:127.0.0.1
EOC

for i in 1 2; do
    openssl req -x509 -newkey rsa:2048 -nodes -batch -sha256 -days $DAYS \
        -subj "/O=ClickHouse Test/CN=Test Root CA $i" -keyout ca$i.key -out ca$i.crt

    openssl req -newkey rsa:2048 -nodes -batch -config leaf.cnf -keyout cert$i.key -out cert$i.csr
    openssl x509 -req -in cert$i.csr -CA ca$i.crt -CAkey ca$i.key -CAcreateserial -sha256 -days $DAYS \
        -extfile leaf.cnf -extensions v3_leaf -out cert$i.crt
    rm -f cert$i.csr ca$i.srl
done
rm -f leaf.cnf

cp ca1.crt ca.crt
cp cert1.crt node.crt
cp cert1.key node.key
