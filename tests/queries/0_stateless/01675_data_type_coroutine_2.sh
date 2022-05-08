#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


counter=0 retries=60
I=0
while [[ $counter -lt $retries ]]; do
    I=$((I + 1))
    TYPE=$(perl -e "print 'Array(' x $I; print 'UInt8'; print ')' x $I")
    ${CLICKHOUSE_CLIENT} --prefer_localhost_replica=0 --max_parser_depth 1000000 --query "SELECT * FROM remote('127.0.0.{1,2}', generateRandom('x $TYPE', 1, 1, 1)) LIMIT 1 FORMAT Null" 2>&1 | grep -q -F 'Maximum parse depth' && break;
    ((++counter))
done

#echo "I = ${I}"
echo 'Ok'

# wait queries, since there is 'Maximum parse depth' error on the client
# and in this case it simply reset the connection and don't read everything
# from server, so there is no guarantee that the query is stopped when the
# client returns
clickhouse_test_wait_queries 60
