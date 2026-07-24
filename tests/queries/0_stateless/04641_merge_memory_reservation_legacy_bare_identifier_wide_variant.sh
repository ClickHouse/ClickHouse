#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / countRebuiltProjectionStreams / tryCountBareIdentifierProjectionSubstreams) on a row-reducing merge that
# rebuilds a bare-identifier projection over a legacy wide part carrying a wide composite Dynamic variant. A
# pre-columns_substreams.txt wide part records nothing, and the estimator used to drop straight to the static
# type-capacity bound as soon as one matching source part lacked recorded substreams - too low here, because the
# legacy part's single materialized variant is a named Tuple whose serialization opens one stream per element,
# more than the fixed per-variant worst case (STREAMS_PER_DYNAMIC_VARIANT). The estimator must instead recover
# the column's real dynamic .bin files from the legacy wide part's disk layout (the same recovery the base merge
# path uses) and union them with the newer parts' recorded substreams.
#
# The legacy part is simulated by deleting columns_substreams.txt from the first part on disk (via a persistent
# clickhouse-local --path so the two runs share the data directory). The projection is added after the parts
# exist, so no part has it materialized, and OPTIMIZE ... FINAL DEDUPLICATE makes the merge row-reducing, which
# REBUILDS the projection from the merged base rows (deduplicate_merge_projection_mode = 'rebuild'). Under a
# pathologically small merges_mutations_memory_usage_soft_limit the merge must still collapse the duplicates to a
# single part with the rebuilt projection intact and must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04641_merge_memory_reservation_legacy_bare_identifier_wide_variant"
DB_PATH="${WORKING_FOLDER}/db"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# First run: three wide parts whose Dynamic(max_types = 1) column carries the same wide named-tuple variant (one
# stream per tuple element, plus discriminators), with the first two parts holding identical rows so DEDUPLICATE
# has something to collapse. STOP MERGES keeps the inserts as separate parts; the large merge_selecting_sleep_ms
# keeps background selection from firing before the explicit OPTIMIZE below. min_bytes_for_wide_part = 0 and
# min_rows_for_wide_part = 0 force the Wide format so the legacy part keeps real per-column .bin files on disk.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    SET enable_dynamic_type = 1;

    CREATE TABLE t_merge_mem_legacy_bare_wide (k UInt64, d Dynamic(max_types = 1))
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             deduplicate_merge_projection_mode = 'rebuild',
             merge_selecting_sleep_ms = 600000, max_merge_selecting_sleep_ms = 600000;

    SYSTEM STOP MERGES t_merge_mem_legacy_bare_wide;
    -- Every row carries the same named-tuple type, so it is the single materialized variant of the
    -- Dynamic(max_types = 1) column and its serialization opens one stream per tuple element - wider than the
    -- fixed per-variant worst case of the type-capacity fallback. The first two parts are identical, so the
    -- deduplicating merge is genuinely row-reducing.
    INSERT INTO t_merge_mem_legacy_bare_wide SELECT number,
        CAST(tuple(number, toString(number), number / 7, [number, number + 1], if(number % 2 = 0, number, NULL), toDate('2026-01-01') + number % 365),
             'Tuple(a UInt64, s String, f Float64, arr Array(UInt64), n Nullable(UInt64), dt Date)')
        FROM numbers(1000);
    INSERT INTO t_merge_mem_legacy_bare_wide SELECT number,
        CAST(tuple(number, toString(number), number / 7, [number, number + 1], if(number % 2 = 0, number, NULL), toDate('2026-01-01') + number % 365),
             'Tuple(a UInt64, s String, f Float64, arr Array(UInt64), n Nullable(UInt64), dt Date)')
        FROM numbers(1000);
    INSERT INTO t_merge_mem_legacy_bare_wide SELECT number,
        CAST(tuple(number, toString(number), number / 7, [number, number + 1], if(number % 2 = 0, number, NULL), toDate('2026-01-01') + number % 365),
             'Tuple(a UInt64, s String, f Float64, arr Array(UInt64), n Nullable(UInt64), dt Date)')
        FROM numbers(1000, 1000);

    -- Add the bare-identifier projection AFTER the parts exist, so none has it materialized and the
    -- row-reducing merge rebuilds it from the merged base rows.
    ALTER TABLE t_merge_mem_legacy_bare_wide ADD PROJECTION p_bare (SELECT k, d ORDER BY k);
"

# Turn the first part (all_1_1_0) into a legacy part by dropping its columns_substreams.txt, so the projection
# column has a matching source part with no recorded substreams and the bare-identifier estimate must recover
# that part's real dynamic .bin files from disk instead of dropping to the static type capacity.
legacy_part=$(find "${DB_PATH}" -type d -name 'all_1_1_0' | head -1)
rm -f "${legacy_part}/columns_substreams.txt"

# Second run: the explicit row-reducing OPTIMIZE ... FINAL DEDUPLICATE reserves memory unconditionally, so under
# the tiny soft limit it must still collapse the duplicate rows to a single part with the rebuilt projection and
# must not error while estimating.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    OPTIMIZE TABLE t_merge_mem_legacy_bare_wide FINAL DEDUPLICATE SETTINGS optimize_throw_if_noop = 1;

    -- The duplicate part collapsed: 2000 distinct rows remain out of 3000.
    SELECT count() FROM t_merge_mem_legacy_bare_wide;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_legacy_bare_wide' AND active;
    -- The rebuilt projection must be present in the single merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_legacy_bare_wide' AND active
        ORDER BY name;
    -- The wide tuple variant must survive the merge intact.
    SELECT sum(CAST(d, 'Tuple(a UInt64, s String, f Float64, arr Array(UInt64), n Nullable(UInt64), dt Date)').a)
        FROM t_merge_mem_legacy_bare_wide;
" -- --merges_mutations_memory_usage_soft_limit=1

rm -rf "${WORKING_FOLDER}"
