#!/usr/bin/env bash
# Tags: no-fasttest
#
# PuffinMetadata footer / required-field error cases. Split from 04257 so CI
# stays under the 300s timeout; cases run in a small parallel pool.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./data_puffin/puffin_errors_common.sh
. "$CURDIR"/data_puffin/puffin_errors_common.sh

launch "$id" meta "$DATA/inflated_lz4_content_size.puffin" 'Puffin footer LZ4 content size'
id=$((id + 1))
launch "$id" meta "$DATA/lz4_content_size_over_absolute_cap.puffin" 'absolute decompression limit'
id=$((id + 1))
launch "$id" meta "$DATA/missing_lz4_content_size.puffin" 'Puffin footer LZ4 frame must declare content size'
id=$((id + 1))
launch "$id" meta "$DATA/lz4_trailing_bytes.puffin" 'trailing bytes'
id=$((id + 1))
launch "$id" meta "$DATA/incomplete_lz4_footer.puffin" 'Puffin footer LZ4 frame is incomplete'
id=$((id + 1))

for f in missing_snapshot_id missing_sequence_number missing_fields missing_type missing_offset missing_length
do
    launch "$id" meta "$DATA/$f.puffin" 'missing required field'
    id=$((id + 1))
done

launch "$id" meta "$DATA/missing_blobs.puffin" "missing required field 'blobs'"
id=$((id + 1))
launch "$id" meta "$DATA/null_blobs.puffin" "missing required field 'blobs'"
id=$((id + 1))

for f in null_blob_entry invalid_blob_entry
do
    launch "$id" meta "$DATA/$f.puffin" 'must be an object'
    id=$((id + 1))
done

for f in missing_properties missing_referenced_data_file missing_cardinality
do
    launch "$id" meta_re "$DATA/$f.puffin" 'missing required (field|property)'
    id=$((id + 1))
done

for f in invalid_properties_array invalid_properties_string
do
    launch "$id" meta "$DATA/$f.puffin" "field 'properties' must be an object"
    id=$((id + 1))
done

for f in invalid_file_properties_array invalid_file_properties_string
do
    launch "$id" meta "$DATA/$f.puffin" "Puffin footer field 'properties' must be an object"
    id=$((id + 1))
done

launch "$id" meta "$DATA/invalid_file_property_number.puffin" "Puffin footer property"
id=$((id + 1))

launch "$id" meta "$DATA/missing_footer_leading_magic.puffin" 'Invalid Puffin footer length'
id=$((id + 1))

finish_puffin_errors
