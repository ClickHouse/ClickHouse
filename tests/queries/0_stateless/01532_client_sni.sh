#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# Check that ClickHouse properly uses SNI extension in Client Hello packet in HTTPS connection.

nc -q0 -l 5678 | xxd -p | grep -oF $'0000000e000c0000096c6f63616c686f7374' &

${CLICKHOUSE_LOCAL} --query "SELECT * FROM url('https://localhost:5678/', RawBLOB, 'data String')" 2>&1 | grep -v -F 'Timeout'

#   grep -oF '\x00\x00\x00\x0e\x00\x0c\x00\x00\x09\x6c\x6f\x63\x61\x6c\x68\x6f\x73\x74'
#              ^^^^^^^^ ^^^^^^^ ^^^^^^^ ^^ ^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#                  |       |      |      |      |
#           server name  data     |   hostname  |   l   o   c   a   l   h   o   s  t
#          extension id  len: 14  |    type     |
#                                 |             |
#                       hostnames list       hostname
#                            len, 12          len, 9

wait
