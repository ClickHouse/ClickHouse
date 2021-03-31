#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TEST_OUTPUT=`$CLICKHOUSE_LOCAL --query "select toUInt8(1) as a, toUInt16(2) as b, '\xDE\xAD\xBE\xEF' as c, toFixedString('\xCA\xFE\xBA\xBE',4) as d from numbers(5) format Native" | xxd -ps`

EXPECTED_OUTPUT=`
echo '040501610555496e7438010101010101620655496e743136020002000200
02000200016306537472696e6704deadbeef04deadbeef04deadbeef04de
adbeef04deadbeef01640e4669786564537472696e67283429cafebabeca
febabecafebabecafebabecafebabe'`

if [ "$TEST_OUTPUT" = "$EXPECTED_OUTPUT" ]; then
    echo 'OK'
else
    echo 'FAIL'
fi
