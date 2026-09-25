#!/bin/bash

# 1. Generate CA's private key and self-signed certificate
openssl req -newkey rsa:4096 -x509 -days 3650 -nodes -batch -keyout ca-key.pem -out ca-cert.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=ca"

# 2. Generate server's private key and certificate signing request (CSR)
openssl req -newkey rsa:4096 -nodes -batch -keyout server-key.pem -out server-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=server"

# 3. Use CA's private key to sign server's CSR and get back the signed certificate
openssl x509 -req -days 3650 -in server-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -extfile server-ext.cnf -out server-cert.pem

# 4. Generate client's private key and certificate signing request (CSR)
openssl req -newkey rsa:4096 -nodes -batch -keyout client1-key.pem -out client1-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client1"
openssl req -newkey rsa:4096 -nodes -batch -keyout client2-key.pem -out client2-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client2"
openssl req -newkey rsa:4096 -nodes -batch -keyout client3-key.pem -out client3-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client3"
openssl req -newkey rsa:4096 -nodes -batch -keyout client4-key.pem -out client4-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client4"
openssl req -newkey rsa:4096 -nodes -batch -keyout client5-key.pem -out client5-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client5"
openssl req -newkey rsa:4096 -nodes -batch -keyout client6-key.pem -out client6-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client6"
# A single-label name under '*.corp.example.com', set as both the CN and a DNS SAN, to test
# that a DNS/CN wildcard matches exactly one label (positive case).
openssl req -newkey rsa:4096 -nodes -batch -keyout client7-key.pem -out client7-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=single.corp.example.com"
# A multi-label name under '*.corp.example.com', set as both the CN and a DNS SAN, to test
# that a DNS/CN wildcard does NOT match more than one label (the authentication-bypass case).
openssl req -newkey rsa:4096 -nodes -batch -keyout client8-key.pem -out client8-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=evil.deep.corp.example.com"
# A URI SAN whose wildcard-matched path segment contains a dot, to lock in that '.' is NOT a
# separator for URI SANs (non-regression of the existing 'URI:spiffe://bar.com/foo/*/far' match).
openssl req -newkey rsa:4096 -nodes -batch -keyout client9-key.pem -out client9-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client9"
# An empty first label ('.corp.example.com'), set as both the CN and a DNS SAN, to test that a
# DNS/CN wildcard does NOT match an empty label: '*.corp.example.com' must reject this (RFC 6125
# requires '*' to match exactly one NON-empty label).
openssl req -newkey rsa:4096 -nodes -batch -keyout client10-key.pem -out client10-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=.corp.example.com"
# A URI SAN with no dot in its host ('URI:spiffe://foo/bar'), to test that a SAN wildcard pattern
# configured without a 'DNS:'/'URI:' prefix does NOT widen URI matching: a bare 'SAN *' must NOT
# match this URI certificate (the '/' separator rejects it, exactly as the original guard did).
openssl req -newkey rsa:4096 -nodes -batch -keyout client11-key.pem -out client11-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client11"
# A name with a '/' in the wildcard-matched span ('foo/bar.corp.example.com'), set as both the CN
# and a DNS SAN, to test that a DNS/CN wildcard does NOT match a span containing '/': the matched
# label must contain neither '.' nor '/', so '*.corp.example.com' must reject this. ('/' in the CN
# is escaped as '\/' because openssl uses '/' as the -subj field separator.)
openssl req -newkey rsa:4096 -nodes -batch -keyout client12-key.pem -out client12-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=foo\/bar.corp.example.com"
# A certificate whose validity period extends beyond the year 2106, i.e. beyond the range of DateTime
# (UInt32 epoch seconds). Used to check that session_log records such validity times without truncation.
openssl req -newkey rsa:4096 -nodes -batch -keyout client_far_future-key.pem -out client_far_future-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client_far_future"

# 5. Use CA's private key to sign client's CSR and get back the signed certificate
openssl x509 -req -days 3650 -in client1-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client1-cert.pem
openssl x509 -req -days 3650 -in client2-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client2-cert.pem
openssl x509 -req -days 3650 -in client3-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client3-cert.pem
openssl x509 -req -days 3650 -in client4-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -extfile client4-ext.cnf -out client4-cert.pem
openssl x509 -req -days 3650 -in client5-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -extfile client5-ext.cnf -out client5-cert.pem
openssl x509 -req -days 3650 -in client6-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -extfile client6-ext.cnf -out client6-cert.pem
openssl x509 -req -days 3650 -in client7-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -extfile client7-ext.cnf -out client7-cert.pem
openssl x509 -req -days 3650 -in client8-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -extfile client8-ext.cnf -out client8-cert.pem
openssl x509 -req -days 3650 -in client9-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -extfile client9-ext.cnf -out client9-cert.pem
openssl x509 -req -days 3650 -in client10-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -extfile client10-ext.cnf -out client10-cert.pem
openssl x509 -req -days 3650 -in client11-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -extfile client11-ext.cnf -out client11-cert.pem
openssl x509 -req -days 3650 -in client12-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -extfile client12-ext.cnf -out client12-cert.pem
# ~100 years, so the notAfter time falls past the year 2106 (the upper bound of DateTime).
openssl x509 -req -days 36525 -in client_far_future-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client_far_future-cert.pem

# 6. Generate one more self-signed certificate and private key for using as wrong certificate (because it's not signed by CA)
openssl req -newkey rsa:4096 -x509 -days 3650 -nodes -batch -keyout wrong-key.pem -out wrong-cert.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client"

# 7. Generate a CA-signed certificate whose CN carries an embedded NUL byte ("client1\0.evil.com"),
# to test that server-side CN extraction does not truncate at the NUL. User 'john' is configured with
# <common_name>client1</common_name>, so a server that truncated the CN at the NUL would extract
# "client1" and wrongly authenticate this certificate as 'john'; the full CN must be preserved so the
# match fails. openssl's CLI cannot place a NUL inside a -subj field, so we build this certificate
# with the 'cryptography' library instead.
python3 - <<'PY'
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import datetime

with open("ca-key.pem", "rb") as f:
    ca_key = serialization.load_pem_private_key(f.read(), password=None)
with open("ca-cert.pem", "rb") as f:
    ca_cert = x509.load_pem_x509_certificate(f.read())

key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
subject = x509.Name([
    x509.NameAttribute(NameOID.COUNTRY_NAME, "RU"),
    x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Some-State"),
    x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Internet Widgits Pty Ltd"),
    x509.NameAttribute(NameOID.COMMON_NAME, "client1\x00.evil.com"),
])
now = datetime.datetime(2020, 1, 1)
cert = (x509.CertificateBuilder()
    .subject_name(subject).issuer_name(ca_cert.subject)
    .public_key(key.public_key()).serial_number(x509.random_serial_number())
    .not_valid_before(now).not_valid_after(now + datetime.timedelta(days=3650))
    .sign(ca_key, hashes.SHA256()))
with open("client13-key.pem", "wb") as f:
    f.write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
with open("client13-cert.pem", "wb") as f:
    f.write(cert.public_bytes(serialization.Encoding.PEM))
PY
