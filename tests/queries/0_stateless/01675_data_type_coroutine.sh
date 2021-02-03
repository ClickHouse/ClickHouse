#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

I=0
while true
do
    I=$((I + 1))
    TYPE=$(perl -e "print 'Array(' x $I; print 'UInt8'; print ')' x $I")
    ${CLICKHOUSE_CLIENT} --max_parser_depth 1000000 --query "SELECT * FROM remote('127.0.0.{1,2}', generateRandom('x $TYPE', 1, 1, 1)) LIMIT 1 FORMAT Null" 2>&1 | grep -q -F 'Maximum parse depth' && break;
done

#echo "I = ${I}"
echo 'Ok'
