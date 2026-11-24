#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p tmp
work_dir=$(mktemp -d "$PWD/tmp/mtls-certs.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

# These keys are test fixtures only. Re-run this script to replace the PEM files.
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout ca-key.pem -out ca-cert.pem -subj '/CN=Interserver mTLS test CA' \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign' > /dev/null 2>&1

make_signed_certificate() {
    local name=$1 subject=$2 serial=$3 usage=$4 sans=$5
    local extension_file="$work_dir/$name.ext"
    {
        printf 'basicConstraints=critical,CA:FALSE\n'
        printf 'keyUsage=critical,digitalSignature,keyEncipherment\n'
        printf 'extendedKeyUsage=%s\n' "$usage"
        if [[ -n "$sans" ]]; then
            printf 'subjectAltName=%s\n' "$sans"
        fi
    } > "$extension_file"

    openssl req -new -newkey rsa:2048 -nodes \
        -keyout "$name-key.pem" -out "$work_dir/$name.csr" -subj "$subject" > /dev/null 2>&1
    openssl x509 -req -in "$work_dir/$name.csr" -CA ca-cert.pem -CAkey ca-key.pem \
        -set_serial "$serial" -days 3650 -out "$name-cert.pem" \
        -extfile "$extension_file" > /dev/null 2>&1
}

make_signed_certificate server '/CN=interserver.test' 0x01 serverAuth 'DNS:interserver.test'
# Put an unlisted SAN before two allowed ones to exercise scanning all SANs.
make_signed_certificate valid-a '/CN=unlisted' 0x02 clientAuth \
    'URI:spiffe://example.org/clickhouse/unlisted,URI:spiffe://example.org/clickhouse/replica-a,URI:spiffe://example.org/clickhouse/replica-b'
make_signed_certificate valid-b '/CN=unlisted' 0x03 clientAuth \
    'URI:spiffe://example.org/clickhouse/replica-b'
make_signed_certificate wrong-san '/CN=unlisted' 0x04 clientAuth \
    'URI:spiffe://example.org/clickhouse/wrong'
make_signed_certificate cn-only '/CN=legacy-replica' 0x05 clientAuth ''
make_signed_certificate no-subjects '/O=No CN or SAN' 0x06 clientAuth ''

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout untrusted-key.pem -out untrusted-cert.pem -subj '/CN=untrusted' \
    -addext 'basicConstraints=critical,CA:FALSE' \
    -addext 'extendedKeyUsage=clientAuth' > /dev/null 2>&1
