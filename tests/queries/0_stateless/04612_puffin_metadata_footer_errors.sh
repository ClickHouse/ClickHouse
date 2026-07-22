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

for f in invalid_properties_array invalid_properties_string null_properties
do
    launch "$id" meta "$DATA/$f.puffin" "field 'properties' must be an object"
    id=$((id + 1))
done

for f in invalid_file_properties_array invalid_file_properties_string null_file_properties
do
    launch "$id" meta "$DATA/$f.puffin" "Puffin footer field 'properties' must be an object"
    id=$((id + 1))
done

launch "$id" meta "$DATA/invalid_file_property_number.puffin" "Puffin footer property"
id=$((id + 1))

launch "$id" meta "$DATA/missing_footer_leading_magic.puffin" 'Invalid Puffin footer length'
id=$((id + 1))

# Oversized raw footer is generated at runtime (sparse) to avoid committing a 16 MiB fixture.
OVERSIZE_FOOTER="$TMP/oversized_raw_footer.puffin"
python3 - "$OVERSIZE_FOOTER" <<'PY'
import struct
import sys

path = sys.argv[1]
magic = b"PFA1"
footer_length = 16 * 1024 * 1024 + 1
flags = b"\x00\x00\x00\x00"
with open(path, "wb") as f:
    f.write(magic)
    f.write(magic)
    f.seek(footer_length - 1, 1)
    f.write(b"{")
    f.write(struct.pack("<i", footer_length))
    f.write(flags)
    f.write(magic)
PY
launch "$id" meta "$OVERSIZE_FOOTER" 'exceeds absolute limit'
id=$((id + 1))

finish_puffin_errors
