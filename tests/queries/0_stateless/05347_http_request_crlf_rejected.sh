#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the AWS SDK (s3 table function)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# CR or LF must never reach an outgoing HTTP request. The S3 access key goes into the
# SigV4 `Authorization` header built by the AWS SDK, without any check on the ClickHouse side.
output=$($CLICKHOUSE_CLIENT -q "SELECT * FROM s3('http://localhost:${CLICKHOUSE_PORT_HTTP}/bucket/key.csv', 'key\r\nX-Injected: 1', 'secret', 'CSV', 'x UInt8')" 2>&1)
if echo "$output" | grep -q "HTTP request header contains CR or LF"; then
    echo "OK"
else
    echo "Unexpected output: $output"
fi
