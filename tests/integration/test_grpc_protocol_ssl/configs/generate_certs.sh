#!/bin/bash

# 1. Generate CA's private key and self-signed certificate
openssl req -newkey rsa:4096 -x509 -days 3650 -nodes -batch -keyout ca-key.pem -out ca-cert.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=ca"

# 2. Generate server's private key and certificate signing request (CSR)
openssl req -newkey rsa:4096 -nodes -batch -keyout server-key.pem -out server-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=server"

# 3. Use CA's private key to sign server's CSR and get back the signed certificate
openssl x509 -req -days 3650 -in server-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -extfile server-ext.cnf -out server-cert.pem

# 4. Generate client's private key and certificate signing request (CSR)
openssl req -newkey rsa:4096 -nodes -batch -keyout client-key.pem -out client-req.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client"

# 5. Use CA's private key to sign client's CSR and get back the signed certificate
openssl x509 -req -days 3650 -in client-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client-cert.pem

# 6. Generate one more self-signed certificate and private key for using as wrong certificate (because it's not signed by CA)
openssl req -newkey rsa:4096 -x509 -days 3650 -nodes -batch -keyout wrong-client-key.pem -out wrong-client-cert.pem -subj "/C=RU/ST=Some-State/O=Internet Widgits Pty Ltd/CN=client"
