#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# Check that ClickHouse properly uses SNI extension in Client Hello packet in HTTPS connection.

strace -f -x -s10000 -e trace=write,sendto ${CLICKHOUSE_LOCAL} --query "SELECT * FROM url('https://yandex.ru/', RawBLOB, 'data String')" 2>&1 |
    grep -oF '\x00\x00\x00\x0e\x00\x0c\x00\x00\x09\x79\x61\x6e\x64\x65\x78\x2e\x72\x75'
#              ^^^^^^^^ ^^^^^^^ ^^^^^^^ ^^ ^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#                  |       |      |      |      |
#           server name  data     |   hostname  |   y   a   n   d   e   x   .   r  u
#          extension id  len: 14  |    type     |
#                                 |             |
#                       hostnames list       hostname
#                            len, 14          len, 9
