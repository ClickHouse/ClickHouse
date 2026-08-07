#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS test_sparse_enum;

    CREATE TABLE test_sparse_enum
    (
        x UInt64,
        y Enum8('a' = 0, 'b' = 1, 'c' = 2),
    )
    ENGINE = MergeTree() ORDER BY x;
"

echo '{"x" : 1}' | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&query=INSERT%20INTO%20test_sparse_enum%20FORMAT%20JSON" --data-binary @-
echo '{"x" : 2}' | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&query=INSERT%20INTO%20test_sparse_enum%20FORMAT%20JSON" --data-binary @-

$CLICKHOUSE_CLIENT -q "
    SELECT * FROM test_sparse_enum ORDER BY x;
    DROP TABLE test_sparse_enum;
"
