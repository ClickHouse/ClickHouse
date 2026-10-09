#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the AWS SDK (s3 table function)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# CR or LF must never reach an outgoing HTTP request. The S3 access key goes into the
# SigV4 `Authorization` header built by the AWS SDK, without any check on the ClickHouse side.
$CLICKHOUSE_CLIENT -q "
    SELECT * FROM s3('http://localhost:${CLICKHOUSE_PORT_HTTP}/bucket/key.csv', 'key\r\nX-Injected: 1', 'secret', 'CSV', 'x UInt8')" 2>&1 \
    | grep -o -m1 "HTTP request header contains CR or LF"

# An unknown storage class is sent as is in `x-amz-storage-class` when `s3_validate_request_settings = 0`,
# so CR or LF in it must be rejected before the request is made.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3('http://localhost:${CLICKHOUSE_PORT_HTTP}/bucket/${CLICKHOUSE_DATABASE}.csv', 'key', 'secret', 'CSV', 'x UInt8',
        storage_class_name = 'STANDARD\r\nX-Injected: 1')
    SETTINGS s3_validate_request_settings = 0, s3_truncate_on_insert = 1
    SELECT 1" 2>&1 | grep -o -m1 "Setting storage_class has invalid value: it contains CR or LF"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_storage_class;
    CREATE TABLE t_storage_class (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_storage_class SELECT 1;
"
$CLICKHOUSE_CLIENT -q "
    BACKUP TABLE t_storage_class TO S3('http://localhost:${CLICKHOUSE_PORT_HTTP}/bucket/${CLICKHOUSE_DATABASE}_backup', 'key', 'secret')
    SETTINGS s3_storage_class = 'STANDARD\r\nX-Injected: 1'" 2>&1 | grep -o -m1 "Setting storage_class has invalid value: it contains CR or LF"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_storage_class"
