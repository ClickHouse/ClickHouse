#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PUFFIN="$CURDIR/data_puffin/spark_deletion_vector.puffin"

# Raw stdin is SeekableReadBuffer with fstat size 0; Puffin must buffer instead of seeking.
echo "--- Puffin stdin ---"
cat "$PUFFIN" | $CLICKHOUSE_LOCAL --input-format Puffin \
    --structure 'referenced_data_file String, deleted_rows Array(UInt64)' \
    -q "SELECT length(deleted_rows)"

echo "--- PuffinMetadata stdin ---"
cat "$PUFFIN" | $CLICKHOUSE_LOCAL --input-format PuffinMetadata \
    --structure 'blob_type String, snapshot_id Int64, sequence_number Int64, fields Array(Int32), offset Int64, length Int64, compression_codec String, properties Map(String, String)' \
    -q "SELECT blob_type, length"
