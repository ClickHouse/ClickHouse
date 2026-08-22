#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_exception;"


create_status=$($CLICKHOUSE_CLIENT --query="
CREATE TABLE test_exception(
    id UInt64
)
ENGINE=MergeTree
ORDER BY All" 2>&1) || true

if [[ $create_status == *"required columns: 'All', available columns: 'id'"* ]]; then
    echo "Query got the right exception message."
else
    echo "Unexpected behavior: $create_status"
    exit 1
fi
