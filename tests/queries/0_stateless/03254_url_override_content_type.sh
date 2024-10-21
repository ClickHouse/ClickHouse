#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

nc -l -p 61845 -q 0 > response.txt &

$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION url('http://localhost:61845/', JSONEachRow, 'x UInt8') SELECT 1" > /dev/null 2>&1

( echo -e "Finish him\n" | nc localhost 61845 ) 2>/dev/null || true

wait

grep "Content-Type" response.txt

nc -l -p 61846 -q 0 > response.txt &

$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION url('http://localhost:61846/', JSONEachRow, 'x UInt8', headers('Content-Type' = 'upyachka')) SELECT 1" > /dev/null 2>&1

( echo -e "Finish him\n" | nc localhost 61846 ) 2>/dev/null || true

wait

grep "Content-Type" response.txt
