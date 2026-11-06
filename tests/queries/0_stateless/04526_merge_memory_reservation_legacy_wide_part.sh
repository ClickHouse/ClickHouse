#!/usr/bin/env bash
# Regression test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / countOutputStreams) on the upgrade path where a source part predates columns_substreams.txt. Such a "legacy"
# wide part records no per-column substreams, so the dynamic substreams of its JSON / Dynamic columns are invisible
# to the per-column union that countOutputStreams builds from the newer parts. When the legacy part's dynamic paths
# are disjoint from the newer parts' (legacy part has 'a', new parts have 'b' / 'c', and the merged part writes all
# of them), the union - which only saw the newer parts - undercounts the result, and the whole-part max floor does
# not close the gap because no single source part is as wide as their union. The estimate must instead add back the
# legacy part's unrecorded dynamic streams (its on-disk stream count minus its non-dynamic columns).
#
# A pre-25.8 legacy wide part is simulated by deleting columns_substreams.txt from an existing part on disk (via a
# persistent clickhouse-local --path so the two runs share the data directory). The merge must still succeed and
# reduce everything to a single part under a pathologically small merges_mutations_memory_usage_soft_limit, without
# erroring while estimating a merge whose result part has more dynamic substreams than any input part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04526_merge_memory_reservation_legacy_wide_part"
DB_PATH="${WORKING_FOLDER}/db"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# First run: create three wide parts with disjoint JSON paths and distinct Dynamic types. The large
# merge_selecting_sleep_ms keeps background merge selection from firing before the explicit OPTIMIZE below,
# while STOP MERGES keeps the three inserts as separate parts. min_bytes_for_wide_part = 0 forces the Wide
# format so the per-substream estimate path is exercised.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    SET enable_json_type = 1;
    SET enable_dynamic_type = 1;

    CREATE TABLE t_merge_mem_legacy (k UInt64, j JSON, d Dynamic)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, merge_selecting_sleep_ms = 600000, max_merge_selecting_sleep_ms = 600000;

    SYSTEM STOP MERGES t_merge_mem_legacy;
    INSERT INTO t_merge_mem_legacy SELECT number, ('{\"a0\": ' || toString(number) || ', \"a1\": ' || toString(number) || '}')::JSON, number::Dynamic FROM numbers(1000);
    INSERT INTO t_merge_mem_legacy SELECT number, ('{\"b0\": ' || toString(number) || ', \"b1\": ' || toString(number) || '}')::JSON, toString(number)::Dynamic FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_legacy SELECT number, ('{\"c0\": [' || toString(number) || '], \"c1\": \"x\"}')::JSON, (number / 2)::Dynamic FROM numbers(2000, 1000);
"

# Turn the first part (all_1_1_0, the one with the disjoint 'a' paths) into a legacy part by dropping its
# columns_substreams.txt, so the reservation estimate has to fall back to its on-disk stream count.
legacy_part=$(find "${DB_PATH}" -type d -name 'all_1_1_0' | head -1)
rm -f "${legacy_part}/columns_substreams.txt"

# Second run: an explicit OPTIMIZE ... FINAL reserves memory unconditionally, so under the tiny soft limit it must
# still merge everything (including the legacy part) down to a single part and must not error while estimating.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    OPTIMIZE TABLE t_merge_mem_legacy FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_legacy;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_legacy' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1

rm -rf "${WORKING_FOLDER}"
