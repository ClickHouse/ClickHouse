#!/usr/bin/env bash
# Tags: no-fasttest

# Regression: SELECT count() over Iceberg deletion vectors must use Parquet
# needOnlyCount (footer / row-group metadata) plus roaring range cardinality,
# not decode data pages. Previously hasAttachedDeletes cleared need_only_count
# for any DV, making DeletionVectorTransform's const-count path unreachable.
# Bucketed / per-row-group spans use the same const-chunk + row_num_offset shape.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CUR_DIR}/data_minio/dv_puffin_warehouse/default/dv_puffin_source"

profile_sum()
{
    local event="$1"
    awk -v e="$event" '$0 ~ e ":" { sum += $(NF-1) } END { print sum+0 }'
}

assert_metadata_only()
{
    local label="$1"
    local output="$2"
    local decode read_rg
    decode=$(printf '%s\n' "$output" | profile_sum ParquetDecodingTasks)
    read_rg=$(printf '%s\n' "$output" | profile_sum ParquetReadRowGroups)
    if [ "$decode" -eq 0 ] && [ "$read_rg" -eq 0 ]; then
        echo "${label}: metadata_only"
    else
        echo "${label}: decoded (ParquetDecodingTasks=${decode} ParquetReadRowGroups=${read_rg})"
    fi
}

# Correct surviving row count (4 deletes from 200).
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM icebergLocal('${TABLE_PATH}')
SETTINGS
    optimize_count_from_files = 1,
    use_cache_for_count_from_files = 0
"

# Count must not decode column pages / open row-group readers.
out=$($CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT count() FROM icebergLocal('${TABLE_PATH}')
SETTINGS
    optimize_count_from_files = 1,
    use_cache_for_count_from_files = 0
" 2>&1)
assert_metadata_only "count" "$out"

# Contrast: disabling count-from-files must decode (proves counters are live).
# Also set optimize_trivial_count_query=0 so a future summary shortcut cannot
# answer without opening Parquet (redundant once position deletes gate the shortcut).
out_full=$($CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT count() FROM icebergLocal('${TABLE_PATH}')
SETTINGS
    optimize_count_from_files = 0,
    optimize_trivial_count_query = 0,
    use_cache_for_count_from_files = 0
" 2>&1)
decode_full=$(printf '%s\n' "$out_full" | profile_sum ParquetDecodingTasks)
if [ "$decode_full" -gt 0 ]; then
    echo "optimize_off: decoded"
else
    echo "optimize_off: unexpectedly_metadata_only"
fi

# Per-row-group needOnlyCount spans (same chunk shape as cluster bucket splits):
# absolute row_num_offset + const defaults, adjusted by DV range cardinality.
out_spans=$($CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT count() FROM icebergLocal('${TABLE_PATH}')
SETTINGS
    optimize_count_from_files = 1,
    use_cache_for_count_from_files = 0,
    max_block_size = 1
" 2>&1)
assert_metadata_only "row_group_spans" "$out_spans"
