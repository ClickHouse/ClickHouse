#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$USER_FILES_PATH/${CLICKHOUSE_DATABASE}
FILE=${CLICKHOUSE_DATABASE}

echo "[\"[1,2,3]trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x Array(UInt32)')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "[\"1970-01-02trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x Date')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "[\"1970-01-02trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x Date32')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "[\"1970-01-01 03:00:01trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x DateTime')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "[\"1970-01-01 03:00:01.0000trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x DateTime64(4)')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "[\"42trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x UInt32')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "[\"42.4242trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x Decimal32(4)')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "[\"255.255.255.255trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x IPv4')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "255.255.255.255trash" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'TSV', 'x IPv4')" 2>&1 | grep -F -q "CANNOT_PARSE_INPUT_ASSERTION_FAILED" && echo 'OK' || echo 'FAIL'

echo "255.255.255.255trash" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'CSV', 'x IPv4')" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'

echo "[\"255.255.255.255trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactEachRow', 'x IPv4')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "[\"0000:0000:0000:0000:0000:ffff:192.168.100.228b1trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x IPv6')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "0000:0000:0000:0000:0000:ffff:192.168.100.228b1trash" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'TSV', 'x IPv6')" 2>&1 | grep -F -q "CANNOT_PARSE_INPUT_ASSERTION_FAILED" && echo 'OK' || echo 'FAIL'

echo "0000:0000:0000:0000:0000:ffff:192.168.100.228b1trash" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'CSV', 'x IPv6')" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'

echo "[\"0000:0000:0000:0000:0000:ffff:192.168.100.228b1trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactEachRow', 'x IPv6')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "[\"{1:2, 2:3}trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x Map(UInt32, UInt32)')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "[\"(1, 2)trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x Tuple(UInt32, UInt32)')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

echo "[\"ed9fd45d-6287-47c1-ad9f-d45d628767c1trash\"]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${FILE}', 'JSONCompactStringsEachRow', 'x UUID')" 2>&1 | grep -F -q "UNEXPECTED_DATA_AFTER_PARSED_VALUE" && echo 'OK' || echo 'FAIL'

rm $DATA_FILE
