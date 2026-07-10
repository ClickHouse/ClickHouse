#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_puffin"

for PUFFIN in \
    "$DATA/overflow_offset_length.puffin" \
    "$DATA/negative_offset.puffin" \
    "$DATA/length_exceeds_file.puffin"
do
    echo "--- $(basename "$PUFFIN") ---"
    $CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$PUFFIN', Puffin)" 2>&1 | grep -oF 'Puffin blob 0: offset/length out of bounds'
done

echo "--- invalid_roaring_bitmap.puffin ---"
$CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$DATA/invalid_roaring_bitmap.puffin', Puffin)" 2>&1 | grep -oF 'Failed to deserialize deletion vector roaring bitmap'

echo "--- invalid_bitmap_key.puffin ---"
$CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$DATA/invalid_bitmap_key.puffin', Puffin)" 2>&1 | grep -oF 'Invalid deletion vector bitmap key'

echo "--- inflated_lz4_content_size.puffin ---"
$CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$DATA/inflated_lz4_content_size.puffin', PuffinMetadata)" 2>&1 | grep -oF 'Puffin footer LZ4 content size'
