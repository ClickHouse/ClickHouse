#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$RESULT="result.dat"
$REFERENCE="01781_general_format_for_parser.reference"
$CLICKHOUSE_LOCAL --query "select toUInt8(1) as a, toUInt16(2) as b, '\xDE\xAD\xBE\xEF' as c, toFixedString('\xCA\xFE\xBA\xBE',4) as d from numbers(5) format Native" | xxd > $RESULT


cmp --silent $RESULT $REFERENCE && echo 'OK' || echo 'FAIL'
rm -rf $RESULT
