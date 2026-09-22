#!/bin/bash

# Self-signed certificate that Keeper presents on its secure port. It is also its own trust
# anchor (CA:TRUE), so a client can point caConfig at it directly.
#
# The subjectAltName has to list every name a client may connect to: `node1` is the cluster
# member name used in use_secure_keeper.xml, and `localhost` is kept because the raft
# configuration and the existing arms use it. Once a dNSName SAN is present OpenSSL ignores
# the common name, so localhost must be listed explicitly rather than left to CN.
openssl req -newkey rsa:2048 -x509 -days 36500 -nodes -batch \
    -keyout server.key -out server.crt \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,DNS:node1" \
    -addext "basicConstraints=critical,CA:TRUE"
