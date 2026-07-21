#!/usr/bin/env bash
# Tags: no-fasttest
#
# PuffinMetadata footer / required-field error cases. Split from 04257 so CI
# stays under the 300s timeout; cases run in a small parallel pool.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_puffin"
PARALLEL="${PUFFIN_ERRORS_PARALLEL:-4}"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

run_case()
{
    local id="$1"
    local file="$2"
    local needle="$3"
    local mode="${4:-F}"
    local out="$TMP/$id.out"

    {
        echo "--- $(basename "$file") ---"
        $CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$file', PuffinMetadata)" 2>&1 \
            | grep -o"${mode}" "$needle" || true
    } > "$out"
}

pids=()
wait_one()
{
    [[ ${#pids[@]} -eq 0 ]] && return
    wait "${pids[0]}" || true
    pids=("${pids[@]:1}")
}

launch()
{
    run_case "$@" &
    pids+=($!)
    while [[ ${#pids[@]} -ge $PARALLEL ]]; do
        wait_one
    done
}

id=0

launch "$id" "$DATA/inflated_lz4_content_size.puffin" 'Puffin footer LZ4 content size'
id=$((id + 1))
launch "$id" "$DATA/lz4_content_size_over_absolute_cap.puffin" 'absolute decompression limit'
id=$((id + 1))
launch "$id" "$DATA/missing_lz4_content_size.puffin" 'Puffin footer LZ4 frame must declare content size'
id=$((id + 1))
launch "$id" "$DATA/lz4_trailing_bytes.puffin" 'trailing bytes'
id=$((id + 1))
launch "$id" "$DATA/incomplete_lz4_footer.puffin" 'Puffin footer LZ4 frame is incomplete'
id=$((id + 1))

for f in missing_snapshot_id missing_sequence_number missing_fields missing_type missing_offset missing_length
do
    launch "$id" "$DATA/$f.puffin" 'missing required field'
    id=$((id + 1))
done

launch "$id" "$DATA/missing_blobs.puffin" "missing required field 'blobs'"
id=$((id + 1))
launch "$id" "$DATA/null_blobs.puffin" "missing required field 'blobs'"
id=$((id + 1))

for f in null_blob_entry invalid_blob_entry
do
    launch "$id" "$DATA/$f.puffin" 'must be an object'
    id=$((id + 1))
done

for f in missing_properties missing_referenced_data_file missing_cardinality
do
    launch "$id" "$DATA/$f.puffin" 'missing required (field|property)' E
    id=$((id + 1))
done

for f in invalid_properties_array invalid_properties_string
do
    launch "$id" "$DATA/$f.puffin" "field 'properties' must be an object"
    id=$((id + 1))
done

for f in invalid_file_properties_array invalid_file_properties_string
do
    launch "$id" "$DATA/$f.puffin" "Puffin footer field 'properties' must be an object"
    id=$((id + 1))
done

launch "$id" "$DATA/invalid_file_property_number.puffin" "Puffin footer property"
id=$((id + 1))

while [[ ${#pids[@]} -gt 0 ]]; do
    wait_one
done

for ((i = 0; i < id; i++)); do
    cat "$TMP/$i.out"
done
