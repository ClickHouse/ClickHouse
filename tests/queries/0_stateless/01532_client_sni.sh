#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# Check that ClickHouse properly uses SNI extension in Client Hello packet in HTTPS connection.

echo -ne 'y\r\n' | strace -f -x -s10000 -e trace=write,sendto ${CLICKHOUSE_LOCAL} --query "SELECT * FROM url('https://localhost:${CLICKHOUSE_PORT_HTTPS}/', RawBLOB, 'data String')" 2>&1 |
    grep -oF '\x00\x00\x00\x0e\x00\x0c\x00\x00\x09\x6c\x6f\x63\x61\x6c\x68\x6f\x73\x74'
#              ^^^^^^^^ ^^^^^^^ ^^^^^^^ ^^ ^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#                  |       |      |      |      |
#           server name  data     |   hostname  |   l   o   c   a   l   h   o   s  t
#          extension id  len: 14  |    type     |
#                                 |             |
#                       hostnames list       hostname
#                            len, 12          len, 9
