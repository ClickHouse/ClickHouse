#!/bin/bash
# Generate TLS certificates for Keeper Raft cert reload test
# 
# This script generates:
# - A self-signed root CA
# - Two server certificates (first, second) signed by the root CA
#
# Both server certs are valid for node1, node2, node3 hostnames
# so they can be used interchangeably across the cluster.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DAYS_VALID=3650  # 10 years for test stability

echo "Generating root CA..."
openssl genrsa -out rootCA.key 2048
openssl req -x509 -new -nodes -key rootCA.key -sha256 -days $DAYS_VALID \
    -out rootCA.pem \
    -subj "/C=US/ST=Test/L=Test/O=ClickHouse Test/CN=Test Root CA"

# Create OpenSSL config for server certs with SANs
cat > server.cnf << 'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = Test
L = Test
O = ClickHouse Test
CN = localhost

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = node1
DNS.3 = node2
DNS.4 = node3
IP.1 = 127.0.0.1
EOF

generate_server_cert() {
    local name=$1
    echo "Generating $name certificate..."
    
    # Generate key
    openssl genrsa -out "${name}.key" 2048
    
    # Generate CSR
    openssl req -new -key "${name}.key" -out "${name}.csr" -config server.cnf
    
    # Sign with root CA
    openssl x509 -req -in "${name}.csr" -CA rootCA.pem -CAkey rootCA.key \
        -CAcreateserial -out "${name}.crt" -days $DAYS_VALID -sha256 \
        -extensions v3_req -extfile server.cnf
    
    # Cleanup CSR
    rm -f "${name}.csr"
    
    echo "Generated ${name}.crt and ${name}.key"
}

# Generate two server certificates
generate_server_cert "first"
generate_server_cert "second"

# Cleanup temporary files
rm -f server.cnf rootCA.key rootCA.srl

echo ""
echo "Certificate generation complete!"
echo "Files generated:"
ls -la *.pem *.crt *.key 2>/dev/null || true

echo ""
echo "Verify certificates:"
echo "  First cert:"
openssl x509 -in first.crt -noout -subject -issuer -dates | head -4
echo "  Second cert:"
openssl x509 -in second.crt -noout -subject -issuer -dates | head -4
