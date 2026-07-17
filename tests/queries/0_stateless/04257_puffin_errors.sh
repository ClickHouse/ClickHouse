#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_puffin"
PUFFIN="$DATA/spark_deletion_vector.puffin"

expect_meta() {
    local file="$1"
    local needle="$2"
    local mode="${3:-F}"
    echo "--- $(basename "$file") ---"
    $CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$file', PuffinMetadata)" 2>&1 | grep -o"${mode}" "$needle"
}

expect_puffin() {
    local file="$1"
    local needle="$2"
    local code="${3:-}"
    echo "--- $(basename "$file") ---"
    local err
    err=$($CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$file', Puffin)" 2>&1)
    echo "$err" | grep -oF "$needle"
    if [[ -n "$code" ]]; then
        echo "$err" | grep -oF "$code"
    fi
}

for f in overflow_offset_length negative_offset length_exceeds_file blob_overlaps_footer
do
    expect_puffin "$DATA/$f.puffin" 'Puffin blob 0: offset/length out of bounds'
done

expect_puffin "$DATA/invalid_roaring_bitmap.puffin" 'Failed to deserialize deletion vector roaring bitmap' 'BAD_ARGUMENTS'
expect_puffin "$DATA/invalid_bitmap_key.puffin" 'Invalid deletion vector bitmap key'
expect_puffin "$DATA/cardinality_mismatch_large_bitmap.puffin" 'exceeds declared cardinality'
expect_puffin "$DATA/dense_cardinality_expansion_bomb.puffin" 'exceeds materialization limit' 'BAD_ARGUMENTS'

for f in invalid_cardinality_non_numeric invalid_cardinality_negative
do
    expect_puffin "$DATA/$f.puffin" "property 'cardinality' must be an unsigned integer"
done

expect_meta "$DATA/inflated_lz4_content_size.puffin" 'Puffin footer LZ4 content size'
expect_meta "$DATA/missing_lz4_content_size.puffin" 'Puffin footer LZ4 frame must declare content size'
expect_meta "$DATA/lz4_trailing_bytes.puffin" 'trailing bytes'
expect_meta "$DATA/incomplete_lz4_footer.puffin" 'Puffin footer LZ4 frame is incomplete'

for f in missing_snapshot_id missing_sequence_number missing_fields missing_type missing_offset missing_length
do
    expect_meta "$DATA/$f.puffin" 'missing required field'
done

expect_meta "$DATA/missing_blobs.puffin" "missing required field 'blobs'"
expect_meta "$DATA/null_blobs.puffin" "missing required field 'blobs'"

for f in null_blob_entry invalid_blob_entry
do
    expect_meta "$DATA/$f.puffin" 'must be an object'
done

for f in missing_properties missing_referenced_data_file missing_cardinality
do
    expect_meta "$DATA/$f.puffin" 'missing required (field|property)' E
done

for f in invalid_properties_array invalid_properties_string
do
    expect_meta "$DATA/$f.puffin" "field 'properties' must be an object"
done

for f in invalid_property_number invalid_property_bool invalid_property_null invalid_property_object \
    invalid_property_cardinality_number
do
    expect_meta "$DATA/$f.puffin" 'must be a string'
done

for f in float_offset float_length float_snapshot_id float_sequence_number float_fields_element string_offset
do
    expect_meta "$DATA/$f.puffin" 'must be an integer'
done

expect_meta "$DATA/fields_element_out_of_int32_range.puffin" 'out of Int32 range'

for f in offset_out_of_int64_range fields_element_out_of_int64_range
do
    expect_meta "$DATA/$f.puffin" 'out of Int64 range'
done

for f in type_number type_bool compression_codec_number compression_codec_bool
do
    expect_meta "$DATA/$f.puffin" 'must be a string'
done

expect_meta "$DATA/footer_root_array.puffin" 'footer JSON must be an object'

for f in malformed_footer_json footer_integer_overflow
do
    expect_meta "$DATA/$f.puffin" 'Cannot parse Puffin footer JSON'
done

for f in invalid_non_dv_properties_array invalid_non_dv_properties_string
do
    expect_meta "$DATA/$f.puffin" "field 'properties' must be an object"
done

expect_meta "$DATA/dv_with_compression_codec.puffin" "must omit 'compression-codec'"

for f in dv_nonzero_snapshot_id dv_nonzero_sequence_number
do
    expect_meta "$DATA/$f.puffin" 'snapshot-id and sequence-number must be -1'
done

echo "--- puffin_wrong_type ---"
$CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$PUFFIN', Puffin, 'deleted_rows Array(String)')" 2>&1 | grep -oF 'Unexpected type'

echo "--- puffin_unknown_column ---"
$CLICKHOUSE_LOCAL -q "SELECT foo FROM file('$PUFFIN', Puffin, 'foo String')" 2>&1 | grep -oF 'Unexpected column'

echo "--- puffin_metadata_wrong_type ---"
$CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$PUFFIN', PuffinMetadata, 'blob_type Int32')" 2>&1 | grep -oF 'Unexpected type'

echo "--- puffin_metadata_unknown_column ---"
$CLICKHOUSE_LOCAL -q "SELECT foo FROM file('$PUFFIN', PuffinMetadata, 'foo String')" 2>&1 | grep -oF 'Unexpected column'
