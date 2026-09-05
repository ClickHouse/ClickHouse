#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / countRebuiltProjectionStreams / countVisibleProjectionColumnStreams) on a row-reducing merge that rebuilds a
# bare-identifier projection whose base column some source parts store under a DIFFERENT declared type - a
# metadata-only widening. tryCountBareIdentifierProjectionSubstreams correctly refuses to treat the by-name union
# as exact on that path, but the estimator used to drop straight to the static type-capacity bound, throwing away
# the streams demonstrably visible in the source parts - which a wide value a source part already materialized
# can exceed (the fixed per-variant worst case, STREAMS_PER_DYNAMIC_VARIANT, is not an upper bound there). The
# rebuilt column must instead be priced at max(type capacity, visible streams), the same treatment
# countOutputStreams applies to a type-widened base column.
#
# The widened source is produced with a lazy JSON type hint (allow_experimental_json_lazy_type_hints makes
# ALTER MODIFY COLUMN metadata-only), so the old parts keep the plain JSON declared type while the table type is
# the wider JSON(a UInt64). One old part is additionally turned into a legacy pre-columns_substreams.txt part by
# deleting that file on disk (via a persistent clickhouse-local --path shared by the two runs), so the visible
# arm exercises both its sources: the recorded union of the parts that still record substreams, and the real
# dynamic .bin files recovered from the unrecorded legacy wide part. The projection is added after the type
# widening, so no part has it materialized, and OPTIMIZE ... FINAL DEDUPLICATE makes the merge row-reducing,
# which REBUILDS it from the merged base rows (deduplicate_merge_projection_mode = 'rebuild'). Under a
# pathologically small merges_mutations_memory_usage_soft_limit the merge must still collapse the duplicates to
# a single part with the rebuilt projection intact and must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04642_merge_memory_reservation_projection_type_widened_source"
DB_PATH="${WORKING_FOLDER}/db"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# First run: three wide parts of plain JSON with several dynamic paths per row (one stream group per path), with
# the first two parts holding identical rows so DEDUPLICATE has something to collapse. STOP MERGES keeps the
# inserts as separate parts; the large merge_selecting_sleep_ms keeps background selection from firing before the
# explicit OPTIMIZE below. min_bytes_for_wide_part = 0 and min_rows_for_wide_part = 0 force the Wide format so
# the legacy part keeps real per-column .bin files on disk. The ALTER then widens the column to JSON(a UInt64)
# metadata-only, so every existing part stores a DIFFERENT declared type than the table, and only afterwards is
# the bare-identifier projection added, so the deduplicating merge rebuilds it from the merged base rows.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    SET enable_json_type = 1;
    SET allow_experimental_json_lazy_type_hints = 1;

    CREATE TABLE t_merge_mem_proj_widened (k UInt64, j JSON)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             deduplicate_merge_projection_mode = 'rebuild',
             merge_selecting_sleep_ms = 600000, max_merge_selecting_sleep_ms = 600000;

    SYSTEM STOP MERGES t_merge_mem_proj_widened;
    INSERT INTO t_merge_mem_proj_widened SELECT number,
        CAST(concat('{\"a\":', toString(number),
                    ',\"s\":\"v', toString(number), '\"',
                    ',\"f\":', toString(number / 7),
                    ',\"arr\":[', toString(number), ',', toString(number + 1), ']',
                    ',\"nested\":{\"x\":', toString(number), ',\"y\":\"z\"}}'), 'JSON')
        FROM numbers(1000);
    INSERT INTO t_merge_mem_proj_widened SELECT number,
        CAST(concat('{\"a\":', toString(number),
                    ',\"s\":\"v', toString(number), '\"',
                    ',\"f\":', toString(number / 7),
                    ',\"arr\":[', toString(number), ',', toString(number + 1), ']',
                    ',\"nested\":{\"x\":', toString(number), ',\"y\":\"z\"}}'), 'JSON')
        FROM numbers(1000);
    INSERT INTO t_merge_mem_proj_widened SELECT number,
        CAST(concat('{\"a\":', toString(number),
                    ',\"s\":\"v', toString(number), '\"',
                    ',\"f\":', toString(number / 7),
                    ',\"arr\":[', toString(number), ',', toString(number + 1), ']',
                    ',\"nested\":{\"x\":', toString(number), ',\"y\":\"z\"}}'), 'JSON')
        FROM numbers(1000, 1000);

    -- Metadata-only widening: existing parts keep the plain JSON declared type.
    ALTER TABLE t_merge_mem_proj_widened MODIFY COLUMN j JSON(a UInt64);

    -- Add the bare-identifier projection AFTER the widening, so the rebuild prices a projection column whose
    -- same-name source parts all store a different declared type.
    ALTER TABLE t_merge_mem_proj_widened ADD PROJECTION p_bare (SELECT k, j ORDER BY k);
"

# Turn the first part (all_1_1_0) into a legacy part by dropping its columns_substreams.txt, so the visible arm
# of the new max(capacity, visible) pricing also recovers real dynamic .bin files from an unrecorded wide part
# of a different declared type, not only the recorded union.
legacy_part=$(find "${DB_PATH}" -type d -name 'all_1_1_0' | head -1)
rm -f "${legacy_part}/columns_substreams.txt"

# Second run: the explicit row-reducing OPTIMIZE ... FINAL DEDUPLICATE reserves memory unconditionally, so under
# the tiny soft limit it must still collapse the duplicate rows to a single part with the rebuilt projection and
# must not error while estimating.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    OPTIMIZE TABLE t_merge_mem_proj_widened FINAL DEDUPLICATE SETTINGS optimize_throw_if_noop = 1;

    -- The duplicate part collapsed: 2000 distinct rows remain out of 3000.
    SELECT count() FROM t_merge_mem_proj_widened;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_proj_widened' AND active;
    -- The rebuilt projection must be present in the single merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_proj_widened' AND active
        ORDER BY name;
    -- The typed path and an old dynamic path must both survive the merge intact.
    SELECT sum(j.a) FROM t_merge_mem_proj_widened;
    SELECT sum(j.nested.x.:Int64) FROM t_merge_mem_proj_widened;
" -- --merges_mutations_memory_usage_soft_limit=1

rm -rf "${WORKING_FOLDER}"
