#!/usr/bin/env bash
# Regression test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / countOutputStreams) when SEVERAL legacy wide parts (parts that predate columns_substreams.txt) physically store
# the SAME JSON dynamic path resolved to the SAME type. The unrecorded-dynamic-streams recovery must union those
# parts' invisible dynamic files by name rather than sum their per-part counts: a shared file name (e.g. the same
# JSON path written as the same type by every part, so ISerialization::getFileNameForStream produces the exact same
# .bin name in each of them) is written only once by the merged part, so counting it once per legacy part would
# over-reserve and can throttle unrelated background merges under merges_mutations_memory_usage_soft_limit for no
# real memory pressure. This test exercises three legacy parts sharing one JSON path and checks the merge still
# succeeds under a pathologically small soft limit.
#
# Pre-25.8 legacy wide parts are simulated by deleting columns_substreams.txt from existing parts on disk (via a
# persistent clickhouse-local --path so the two runs share the data directory).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04549_merge_memory_reservation_legacy_shared_dynamic_path"
DB_PATH="${WORKING_FOLDER}/db"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# First run: create three wide parts that all store the same JSON path 'a' as the same (numeric) type. The large
# merge_selecting_sleep_ms keeps background merge selection from firing before the explicit OPTIMIZE below, while
# STOP MERGES keeps the three inserts as separate parts. min_bytes_for_wide_part = 0 forces the Wide format so the
# per-substream estimate path is exercised.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    SET enable_json_type = 1;

    CREATE TABLE t_merge_mem_legacy_shared (k UInt64, j JSON)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, merge_selecting_sleep_ms = 600000, max_merge_selecting_sleep_ms = 600000;

    SYSTEM STOP MERGES t_merge_mem_legacy_shared;
    INSERT INTO t_merge_mem_legacy_shared SELECT number, ('{\"a\": ' || toString(number) || '}')::JSON FROM numbers(1000);
    INSERT INTO t_merge_mem_legacy_shared SELECT number, ('{\"a\": ' || toString(number) || '}')::JSON FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_legacy_shared SELECT number, ('{\"a\": ' || toString(number) || '}')::JSON FROM numbers(2000, 1000);
"

# Turn all three parts into legacy parts by dropping their columns_substreams.txt, so the reservation estimate has
# to fall back to their on-disk stream counts and run the unrecorded-dynamic-streams recovery for every one of them.
while IFS= read -r legacy_part; do
    rm -f "${legacy_part}/columns_substreams.txt"
done < <(find "${DB_PATH}" -type d -regex '.*/all_[0-9]+_[0-9]+_0')

# Second run: an explicit OPTIMIZE ... FINAL reserves memory unconditionally, so under the tiny soft limit it must
# still merge everything (all three legacy parts sharing path 'a') down to a single part and must not error.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    OPTIMIZE TABLE t_merge_mem_legacy_shared FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_legacy_shared;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_legacy_shared' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1

rm -rf "${WORKING_FOLDER}"
