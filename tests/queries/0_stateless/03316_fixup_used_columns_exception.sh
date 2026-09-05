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

# The key expression is now analyzed through the Analyzer (regardless of the
# `enable_analyzer` setting), so the diagnostic deterministically names the unknown
# identifier `All` and carries the `UNKNOWN_IDENTIFIER` error code in both analyzer modes.
if [[ $create_status == *"Unknown expression identifier"*"All"*"UNKNOWN_IDENTIFIER"* ]]; then
    echo "Query got the right exception message."
else
    echo "Unexpected behavior: $create_status"
    exit 1
fi
