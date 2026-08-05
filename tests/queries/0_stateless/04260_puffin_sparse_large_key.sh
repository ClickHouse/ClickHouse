#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PUFFIN="$CURDIR/data_puffin/sparse_large_key.puffin"

echo "--- sparse_large_key.puffin ---"
$CLICKHOUSE_LOCAL -q "
SELECT deleted_rows
FROM file('$PUFFIN', Puffin)
"

echo "--- ARRAY JOIN ---"
$CLICKHOUSE_LOCAL -q "
SELECT row_number
FROM file('$PUFFIN', Puffin)
ARRAY JOIN deleted_rows AS row_number
"
