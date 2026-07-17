#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PUFFIN="$CURDIR/data_puffin/spark_deletion_vector.puffin"

# When seeks are disabled, Puffin must use the full-buffer fallback (like Arrow/ORC).
echo "--- Puffin allow_seeks=0 ---"
$CLICKHOUSE_LOCAL -q "
SELECT length(deleted_rows)
FROM file('$PUFFIN', Puffin)
SETTINGS input_format_allow_seeks = 0
"

echo "--- PuffinMetadata allow_seeks=0 ---"
$CLICKHOUSE_LOCAL -q "
SELECT blob_type, length
FROM file('$PUFFIN', PuffinMetadata)
SETTINGS input_format_allow_seeks = 0
"
