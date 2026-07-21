#!/usr/bin/env bash
# Tags: no-fasttest
#
# PuffinMetadata JSON typing / DV metadata-rule error cases. Split from 04257
# so CI stays under the 300s timeout; cases run in a small parallel pool.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_puffin"
PUFFIN="$DATA/spark_deletion_vector.puffin"
PARALLEL="${PUFFIN_ERRORS_PARALLEL:-4}"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

run_case()
{
    local id="$1"
    local kind="$2"
    local path_or_label="$3"
    local needle="$4"
    local extra="${5:-}"
    local out="$TMP/$id.out"
    local err

    {
        case "$kind" in
            meta)
                echo "--- $(basename "$path_or_label") ---"
                $CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$path_or_label', PuffinMetadata)" 2>&1 \
                    | grep -oF "$needle" || true
                ;;
            raw_meta)
                echo "--- $path_or_label ---"
                err=$($CLICKHOUSE_LOCAL -q "$extra" 2>&1) || true
                echo "$err" | grep -oF "$needle" || true
                ;;
        esac
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

for f in invalid_property_number invalid_property_bool invalid_property_null invalid_property_object \
    invalid_property_cardinality_number
do
    launch "$id" meta "$DATA/$f.puffin" 'must be a string'
    id=$((id + 1))
done

for f in float_offset float_length float_snapshot_id float_sequence_number float_fields_element string_offset
do
    launch "$id" meta "$DATA/$f.puffin" 'must be an integer'
    id=$((id + 1))
done

launch "$id" meta "$DATA/fields_element_out_of_int32_range.puffin" 'out of Int32 range'
id=$((id + 1))

for f in offset_out_of_int64_range fields_element_out_of_int64_range
do
    launch "$id" meta "$DATA/$f.puffin" 'out of Int64 range'
    id=$((id + 1))
done

for f in type_number type_bool compression_codec_number compression_codec_bool
do
    launch "$id" meta "$DATA/$f.puffin" 'must be a string'
    id=$((id + 1))
done

launch "$id" meta "$DATA/footer_root_array.puffin" 'footer JSON must be an object'
id=$((id + 1))

for f in malformed_footer_json footer_integer_overflow
do
    launch "$id" meta "$DATA/$f.puffin" 'Cannot parse Puffin footer JSON'
    id=$((id + 1))
done

for f in invalid_non_dv_properties_array invalid_non_dv_properties_string
do
    launch "$id" meta "$DATA/$f.puffin" "field 'properties' must be an object"
    id=$((id + 1))
done

launch "$id" meta "$DATA/dv_with_compression_codec.puffin" "must omit 'compression-codec'"
id=$((id + 1))

for f in dv_nonzero_snapshot_id dv_nonzero_sequence_number
do
    launch "$id" meta "$DATA/$f.puffin" 'snapshot-id and sequence-number must be -1'
    id=$((id + 1))
done

launch "$id" raw_meta 'puffin_metadata_wrong_type' 'Unexpected type' \
    "SELECT blob_type FROM file('$PUFFIN', PuffinMetadata, 'blob_type Int32')"
id=$((id + 1))
launch "$id" raw_meta 'puffin_metadata_unknown_column' 'Unexpected column' \
    "SELECT foo FROM file('$PUFFIN', PuffinMetadata, 'foo String')"
id=$((id + 1))

while [[ ${#pids[@]} -gt 0 ]]; do
    wait_one
done

for ((i = 0; i < id; i++)); do
    cat "$TMP/$i.out"
done
