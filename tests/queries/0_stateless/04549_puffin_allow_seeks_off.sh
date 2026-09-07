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

# Non-seekable path must reject a wrong leading magic before buffering the rest of the stream.
echo "--- non-Puffin magic allow_seeks=0 ---"
NOT_PUFFIN="${CLICKHOUSE_TMP}/04549_not_puffin.bin"
# Prefix is wrong; trailing bytes would only matter if we buffered first then validated.
printf 'NOTA' > "$NOT_PUFFIN"
dd if=/dev/zero bs=1024 count=1024 status=none >> "$NOT_PUFFIN"
$CLICKHOUSE_LOCAL -q "
SELECT blob_type
FROM file('$NOT_PUFFIN', PuffinMetadata)
SETTINGS input_format_allow_seeks = 0
" 2>&1 | grep -oF 'Invalid Puffin magic (header)'
