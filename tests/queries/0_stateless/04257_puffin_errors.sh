#!/usr/bin/env bash
# Tags: no-fasttest
#
# Deletion-vector / Puffin payload error cases. Split from the former monolithic
# 04257 suite so slow CI builds stay under the 300s timeout. Cases still run in
# a small parallel pool because clickhouse-local startup dominates runtime.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./data_puffin/puffin_errors_common.sh
. "$CURDIR"/data_puffin/puffin_errors_common.sh

for f in overflow_offset_length negative_offset length_exceeds_file blob_overlaps_footer
do
    launch "$id" puffin "$DATA/$f.puffin" 'Puffin blob 0: offset/length out of bounds'
    id=$((id + 1))
done

launch "$id" puffin "$DATA/invalid_roaring_bitmap.puffin" 'Failed to deserialize deletion vector roaring bitmap' 'BAD_ARGUMENTS'
id=$((id + 1))
launch "$id" puffin "$DATA/invalid_bitmap_key.puffin" 'Invalid deletion vector bitmap key'
id=$((id + 1))
launch "$id" puffin "$DATA/cardinality_mismatch_large_bitmap.puffin" 'exceeds declared cardinality'
id=$((id + 1))
launch "$id" puffin "$DATA/cardinality_exceeds_materialization_limit.puffin" 'exceeds materialization limit' 'BAD_ARGUMENTS'
id=$((id + 1))

# Oversized DV blob region is generated at runtime (sparse) to avoid committing a 2 GiB fixture.
OVERSIZE_DV_BLOB="$TMP/oversized_dv_blob.puffin"
python3 - "$OVERSIZE_DV_BLOB" <<'PY'
import json
import struct
import sys

path = sys.argv[1]
magic = b"PFA1"
blob_length = 2 * 1024 * 1024 * 1024 + 1
footer = {
    "blobs": [
        {
            "type": "deletion-vector-v1",
            "fields": [],
            "snapshot-id": -1,
            "sequence-number": -1,
            "offset": 4,
            "length": blob_length,
            "properties": {
                "referenced-data-file": "/data/table/part-00000.parquet",
                "cardinality": "1",
            },
        }
    ]
}
footer_json = json.dumps(footer, separators=(", ", ": ")).encode("utf-8")
flags = b"\x00\x00\x00\x00"
with open(path, "wb") as f:
    f.write(magic)
    f.seek(4 + blob_length - 1, 0)
    f.write(b"\x00")
    f.write(magic)
    f.write(footer_json)
    f.write(struct.pack("<i", len(footer_json)))
    f.write(flags)
    f.write(magic)
PY
launch "$id" puffin "$OVERSIZE_DV_BLOB" 'exceeds absolute limit'
id=$((id + 1))

for f in invalid_cardinality_non_numeric invalid_cardinality_negative
do
    launch "$id" puffin "$DATA/$f.puffin" "property 'cardinality' must be an unsigned integer"
    id=$((id + 1))
done

# Footer metadata validity must not depend on projecting deleted_rows.
for f in invalid_cardinality_non_numeric invalid_cardinality_negative
do
    launch "$id" raw_puffin "${f}_subset" "property 'cardinality' must be an unsigned integer" \
        "SELECT referenced_data_file FROM file('$DATA/$f.puffin', Puffin)"
    id=$((id + 1))
done

launch "$id" raw_puffin 'puffin_wrong_type' 'Unexpected type' \
    "SELECT deleted_rows FROM file('$PUFFIN', Puffin, 'deleted_rows Array(String)')"
id=$((id + 1))
launch "$id" raw_puffin 'puffin_unknown_column' 'Unexpected column' \
    "SELECT foo FROM file('$PUFFIN', Puffin, 'foo String')"
id=$((id + 1))

finish_puffin_errors
