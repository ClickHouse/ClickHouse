#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_puffin"
PUFFIN="$DATA/spark_deletion_vector.puffin"

for PUFFIN_FILE in \
    "$DATA/overflow_offset_length.puffin" \
    "$DATA/negative_offset.puffin" \
    "$DATA/length_exceeds_file.puffin" \
    "$DATA/blob_overlaps_footer.puffin"
do
    echo "--- $(basename "$PUFFIN_FILE") ---"
    $CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$PUFFIN_FILE', Puffin)" 2>&1 | grep -oF 'Puffin blob 0: offset/length out of bounds'
done

echo "--- invalid_roaring_bitmap.puffin ---"
$CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$DATA/invalid_roaring_bitmap.puffin', Puffin)" 2>&1 | grep -oF 'Failed to deserialize deletion vector roaring bitmap'

echo "--- invalid_bitmap_key.puffin ---"
$CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$DATA/invalid_bitmap_key.puffin', Puffin)" 2>&1 | grep -oF 'Invalid deletion vector bitmap key'

echo "--- cardinality_mismatch_large_bitmap.puffin ---"
$CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$DATA/cardinality_mismatch_large_bitmap.puffin', Puffin)" 2>&1 | grep -oF 'exceeds declared cardinality'

echo "--- inflated_lz4_content_size.puffin ---"
$CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$DATA/inflated_lz4_content_size.puffin', PuffinMetadata)" 2>&1 | grep -oF 'Puffin footer LZ4 content size'

for PUFFIN_FILE in \
    "$DATA/missing_snapshot_id.puffin" \
    "$DATA/missing_sequence_number.puffin" \
    "$DATA/missing_fields.puffin" \
    "$DATA/missing_type.puffin" \
    "$DATA/missing_offset.puffin" \
    "$DATA/missing_length.puffin"
do
    echo "--- $(basename "$PUFFIN_FILE") ---"
    $CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$PUFFIN_FILE', PuffinMetadata)" 2>&1 | grep -oF 'missing required field'
done

echo "--- missing_blobs.puffin ---"
$CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$DATA/missing_blobs.puffin', PuffinMetadata)" 2>&1 | grep -oF "missing required field 'blobs'"

echo "--- null_blobs.puffin ---"
$CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$DATA/null_blobs.puffin', PuffinMetadata)" 2>&1 | grep -oF "missing required field 'blobs'"

for PUFFIN_FILE in \
    "$DATA/null_blob_entry.puffin" \
    "$DATA/invalid_blob_entry.puffin"
do
    echo "--- $(basename "$PUFFIN_FILE") ---"
    $CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$PUFFIN_FILE', PuffinMetadata)" 2>&1 | grep -oF 'must be an object'
done

for PUFFIN_FILE in \
    "$DATA/missing_properties.puffin" \
    "$DATA/missing_referenced_data_file.puffin" \
    "$DATA/missing_cardinality.puffin"
do
    echo "--- $(basename "$PUFFIN_FILE") ---"
    $CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$PUFFIN_FILE', PuffinMetadata)" 2>&1 | grep -oE 'missing required (field|property)'
done

for PUFFIN_FILE in \
    "$DATA/invalid_properties_array.puffin" \
    "$DATA/invalid_properties_string.puffin"
do
    echo "--- $(basename "$PUFFIN_FILE") ---"
    $CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$PUFFIN_FILE', PuffinMetadata)" 2>&1 | grep -oF "field 'properties' must be an object"
done

for PUFFIN_FILE in \
    "$DATA/invalid_non_dv_properties_array.puffin" \
    "$DATA/invalid_non_dv_properties_string.puffin"
do
    echo "--- $(basename "$PUFFIN_FILE") ---"
    $CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$PUFFIN_FILE', PuffinMetadata)" 2>&1 | grep -oF "field 'properties' must be an object"
done

echo "--- dv_with_compression_codec.puffin ---"
$CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$DATA/dv_with_compression_codec.puffin', PuffinMetadata)" 2>&1 | grep -oF "must omit 'compression-codec'"

echo "--- puffin_wrong_type ---"
$CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$PUFFIN', Puffin, 'deleted_rows Array(String)')" 2>&1 | grep -oF 'Unexpected type'

echo "--- puffin_unknown_column ---"
$CLICKHOUSE_LOCAL -q "SELECT foo FROM file('$PUFFIN', Puffin, 'foo String')" 2>&1 | grep -oF 'Unexpected column'

echo "--- puffin_metadata_wrong_type ---"
$CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$PUFFIN', PuffinMetadata, 'blob_type Int32')" 2>&1 | grep -oF 'Unexpected type'

echo "--- puffin_metadata_unknown_column ---"
$CLICKHOUSE_LOCAL -q "SELECT foo FROM file('$PUFFIN', PuffinMetadata, 'foo String')" 2>&1 | grep -oF 'Unexpected column'
