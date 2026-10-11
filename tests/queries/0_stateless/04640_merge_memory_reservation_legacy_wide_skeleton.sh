#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / countOutputStreams) on the type-widened base path when EVERY source part storing the column is a legacy wide
# part without columns_substreams.txt. A widened column is priced at max(type capacity, source-visible streams),
# and the source-visible arm is built from the recorded substream union plus the dynamic .bin files recovered from
# legacy wide parts - files collected AFTER subtracting the column's static skeleton (so a recorded union, which
# contains the skeleton, never double-counts it). When no part records the column at all, that subtraction used to
# leave the visible arm with just the dynamic remainder - the source skeleton was missing - so a legacy part that
# already materialized more streams than the fixed per-variant capacity could still be under-reserved. The visible
# arm must charge the column's static skeleton once (countColumnStreams) whenever there is no recorded union to
# carry it.
#
# A pre-25.8 legacy wide part is simulated by deleting columns_substreams.txt from ALL parts on disk (via a
# persistent clickhouse-local --path so the two runs share the data directory); the lazy JSON type hint keeps
# ALTER MODIFY COLUMN metadata-only, so the first part stores plain JSON while the table type is the wider
# JSON(a UInt64) - a type-widened column whose only stream information is legacy .bin files. The first part's
# JSON carries a wide composite value (a nested object fanning out into many dynamic paths), wider than the fixed
# per-variant worst case. Under a pathologically small merges_mutations_memory_usage_soft_limit the explicit
# OPTIMIZE ... FINAL reserves unconditionally, so it must still merge everything down to a single Wide part and
# must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04640_merge_memory_reservation_legacy_wide_skeleton"
DB_PATH="${WORKING_FOLDER}/db"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# First run: one wide part with plain JSON carrying a wide composite value, then a metadata-only widen to
# JSON(a UInt64) via the lazy type hint, then two more wide parts of the hinted type. STOP MERGES keeps the three
# inserts as separate parts; the large merge_selecting_sleep_ms keeps background selection from firing before the
# explicit OPTIMIZE below. min_bytes_for_wide_part = 0 and min_rows_for_wide_part = 0 force the Wide format so the
# per-substream estimate path is exercised.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    SET enable_json_type = 1;
    SET allow_experimental_json_lazy_type_hints = 1;

    CREATE TABLE t_merge_mem_legacy_skel (k UInt64, json JSON)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             merge_selecting_sleep_ms = 600000, max_merge_selecting_sleep_ms = 600000;

    SYSTEM STOP MERGES t_merge_mem_legacy_skel;
    -- A wide composite value: the nested object fans out into many dynamic paths (one stream each), so this
    -- part's real on-disk stream count exceeds the fixed per-variant worst case of the capacity fallback.
    INSERT INTO t_merge_mem_legacy_skel SELECT number,
        ('{\"a\": ' || toString(number) || ', \"w\": {\"p0\": ' || toString(number) || ', \"p1\": \"' || toString(number) || '\", \"p2\": [' || toString(number) || '], \"p3\": ' || toString(number / 7) || ', \"p4\": ' || toString(number) || ', \"p5\": \"x\", \"p6\": [' || toString(number) || ', 1], \"p7\": ' || toString(number) || '}}')::JSON
        FROM numbers(1000);

    -- Metadata-only widen: the first part keeps plain JSON on disk, newer parts use the hinted type, so the
    -- merged output column differs from the first part's stored type and is capacity-priced.
    ALTER TABLE t_merge_mem_legacy_skel MODIFY COLUMN json JSON(a UInt64);

    INSERT INTO t_merge_mem_legacy_skel SELECT number, ('{\"a\": ' || toString(number) || ', \"b\": ' || toString(number * 2) || '}')::JSON FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_legacy_skel SELECT number, ('{\"a\": ' || toString(number) || ', \"c\": ' || toString(number * 3) || '}')::JSON FROM numbers(2000, 1000);
"

# Turn EVERY part into a legacy part by dropping columns_substreams.txt, so no part records the column and the
# source-visible arm of the widened column's max() is built purely from recovered legacy .bin files - the case
# where the static skeleton must be charged explicitly.
find "${DB_PATH}" -type f -name 'columns_substreams.txt' -delete

# Second run: an explicit OPTIMIZE ... FINAL reserves memory unconditionally, so under the tiny soft limit it must
# still merge everything (all-legacy parts included) down to a single Wide part and must not error while estimating.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    OPTIMIZE TABLE t_merge_mem_legacy_skel FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_legacy_skel;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_legacy_skel' AND active;
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_legacy_skel' AND active;
    -- The typed path and the wide composite paths must survive the merge intact.
    SELECT sum(json.a) FROM t_merge_mem_legacy_skel;
    SELECT sum(json.w.p0.:Int64) FROM t_merge_mem_legacy_skel;
" -- --merges_mutations_memory_usage_soft_limit=1

rm -rf "${WORKING_FOLDER}"
