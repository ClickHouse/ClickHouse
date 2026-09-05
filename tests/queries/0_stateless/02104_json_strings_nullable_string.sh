#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_USER_FILES_UNIQUE/test_02104_null.data

echo -e '{"s" : "NULLSome string"}' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/test_02104_null.data', 'JSONStringsEachRow', 's Nullable(String)')"

echo -e '["NULLSome string"]' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/test_02104_null.data', 'JSONCompactStringsEachRow', 's Nullable(String)')"

rm $DATA_FILE
