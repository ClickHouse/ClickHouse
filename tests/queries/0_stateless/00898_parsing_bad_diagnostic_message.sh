#!/usr/bin/env bash

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

echo -ne '0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\ta' | $CLICKHOUSE_LOCAL --structure 'c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8, c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8, c10 UInt8, c11 UInt8' --input-format TSV --query 'SELECT * FROM table' 2>&1 | grep -F 'Column 11'
