#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_puffin"

run_happy_path() {
    local name="$1"
    local puffin="$2"
    echo "--- $name ---"
    $CLICKHOUSE_LOCAL -q "
    SELECT blob_type, snapshot_id, sequence_number, offset, length, compression_codec, mapKeys(properties), mapValues(properties)
    FROM file('$puffin', PuffinMetadata)
    "
    $CLICKHOUSE_LOCAL -q "
    SELECT referenced_data_file, deleted_rows
    FROM file('$puffin', Puffin)
    "
    $CLICKHOUSE_LOCAL -q "
    SELECT referenced_data_file, row_number
    FROM file('$puffin', Puffin)
    ARRAY JOIN deleted_rows AS row_number
    ORDER BY referenced_data_file, row_number
    "
}

run_happy_path "spark_deletion_vector.puffin" "$DATA/spark_deletion_vector.puffin"
run_happy_path "compressed_footer.puffin" "$DATA/compressed_footer.puffin"
run_happy_path "mixed_blob_types.puffin" "$DATA/mixed_blob_types.puffin"
