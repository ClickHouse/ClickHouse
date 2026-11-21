#!/usr/bin/env bash
# Tags: no-fasttest
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / countOutputStreams) when a source part still carries, on disk, a JSON / Dynamic column that a metadata-only
# ALTER DROP COLUMN removed from the current metadata. IMergeTreeDataPart::loadColumns keeps the source part's own
# columns.txt, so between dropping a column and the lazy cleanup rewriting the part, a merge can select a part that
# still stores the dead column while the merged part writes only the current metadata's columns. countOutputStreams
# must ignore such a source column that is absent from the output columns - both in the legacy wide-part .bin
# recovery and in the whole-part floor - otherwise it reserves memory for wide semi-structured columns the merge
# never writes, saturating merges_mutations_memory_usage_soft_limit and serializing background merges.
#
# The dropped-but-still-on-disk state is reproduced deterministically (a real ALTER DROP COLUMN races its own
# cleanup mutation) by removing the column from the table's metadata SQL between two runs of a persistent
# clickhouse-local --path, leaving the parts' data files untouched. all_1_1_0 is additionally turned into a legacy
# wide part (columns_substreams.txt deleted) so the .bin recovery path runs over the dead column too. Under a
# pathologically small merges_mutations_memory_usage_soft_limit the explicit OPTIMIZE ... FINAL reserves
# unconditionally, so it must still merge everything down to a single part and must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04616_merge_memory_reservation_dropped_column"
DB_PATH="${WORKING_FOLDER}/db"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# First run: three wide parts, each carrying a kept JSON column and a to-be-dropped JSON column with its own dynamic
# paths on disk. STOP MERGES plus a large merge_selecting_sleep_ms keeps them as three separate parts until the
# explicit OPTIMIZE below; min_bytes_for_wide_part = 0 forces the Wide format.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    SET enable_json_type = 1;

    CREATE TABLE t_merge_mem_dropped_column (k UInt64, keep JSON, dead JSON)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, merge_selecting_sleep_ms = 600000, max_merge_selecting_sleep_ms = 600000;

    SYSTEM STOP MERGES t_merge_mem_dropped_column;
    INSERT INTO t_merge_mem_dropped_column SELECT number, toJSONString(map('a', number)), toJSONString(map('d', number)) FROM numbers(1000);
    INSERT INTO t_merge_mem_dropped_column SELECT number, toJSONString(map('a', number)), toJSONString(map('d', number)) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_dropped_column SELECT number, toJSONString(map('a', number)), toJSONString(map('d', number)) FROM numbers(2000, 1000);
"

# Turn all_1_1_0 into a legacy wide part (drop columns_substreams.txt) so the reservation estimate falls back to the
# on-disk .bin recovery, which must not pick up the dead column's files.
legacy_part=$(find "${DB_PATH}" -type d -name 'all_1_1_0' | head -1)
rm -f "${legacy_part}/columns_substreams.txt"

# Remove the dead column from the table metadata only, leaving every part's data (and columns.txt) intact - the exact
# on-disk state a metadata-only ALTER DROP COLUMN leaves before its cleanup mutation rewrites the parts.
table_sql=$(find "${DB_PATH}" -name 't_merge_mem_dropped_column.sql' | head -1)
# Single quotes are intentional: the pattern must match the literal backtick-quoted column name, not expand.
# shellcheck disable=SC2016
sed -i '/`dead` JSON/d' "${table_sql}"

# Second run: metadata now has only (k, keep) while the parts still store dead on disk. The explicit OPTIMIZE ...
# FINAL reserves unconditionally, so under the tiny soft limit it must still merge everything down to one part and
# must not error while estimating, and must not reserve for the dead column.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    SET enable_json_type = 1;

    OPTIMIZE TABLE t_merge_mem_dropped_column FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_dropped_column;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_dropped_column' AND active;
    -- The dead column is gone from the current metadata but the merge still succeeded.
    SELECT arraySort(groupArray(name)) FROM system.columns WHERE database = currentDatabase() AND table = 't_merge_mem_dropped_column';
    -- The kept JSON column still answers queries correctly after the merge.
    SELECT sum(keep.a.:Int64) FROM t_merge_mem_dropped_column;
" -- --merges_mutations_memory_usage_soft_limit=1

rm -rf "${WORKING_FOLDER}"
