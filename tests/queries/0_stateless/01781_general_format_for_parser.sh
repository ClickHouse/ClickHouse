#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TEST_OUTPUT=`$CLICKHOUSE_LOCAL --query "select toUInt8(1) as a, toUInt16(2) as b, '\xDE\xAD\xBE\xEF' as c, toFixedString('\xCA\xFE\xBA\xBE',4) as d from numbers(5) format Native" | md5sum | cut -d ' ' -f1`

EXPECTED_OUTPUT=`echo 'd7255e6863ef058aa60064fab921fb2e'`

if [ "$TEST_OUTPUT" = "$EXPECTED_OUTPUT" ]; then
    echo 'OK'
else
    echo 'FAIL'
fi
