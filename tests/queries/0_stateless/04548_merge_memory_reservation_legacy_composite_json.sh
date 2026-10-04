#!/usr/bin/env bash
# Regression test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / countOutputStreams) on a legacy wide part (one that predates columns_substreams.txt) whose dynamic structure
# lives inside composite columns such as Tuple(UInt64, JSON) and Array(JSON). Such a composite has a real static
# skeleton - the tuple's UInt64 element stream, the array's offsets, the JSON shared-data streams - that the
# default serialization can enumerate and that the per-column union therefore already counts once. The
# unrecorded-dynamic-streams recovery must subtract those statically enumerable streams from the part's on-disk
# stream count and add back only the truly invisible dynamic substreams; subtracting only whole columns without
# dynamic structure would count the composite's static skeleton twice and over-reserve upgrade-path merges.
#
# A pre-25.8 legacy wide part is simulated by deleting columns_substreams.txt from an existing part on disk (via a
# persistent clickhouse-local --path so the two runs share the data directory). Under a pathologically small
# merges_mutations_memory_usage_soft_limit the explicit OPTIMIZE ... FINAL must still reduce everything to one part
# and must not error while estimating a merge of a legacy part holding JSON inside Tuple and Array.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04548_merge_memory_reservation_legacy_composite_json"
DB_PATH="${WORKING_FOLDER}/db"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# First run: create three wide parts, each carrying disjoint JSON paths inside the composite columns. The large
# merge_selecting_sleep_ms keeps background merge selection from firing before the explicit OPTIMIZE below, while
# STOP MERGES keeps the three inserts as separate parts. min_bytes_for_wide_part = 0 forces the Wide format so the
# per-substream estimate path is exercised.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    SET enable_json_type = 1;

    CREATE TABLE t_merge_mem_legacy_composite (k UInt64, t Tuple(n UInt64, j JSON), a Array(JSON))
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, merge_selecting_sleep_ms = 600000, max_merge_selecting_sleep_ms = 600000;

    SYSTEM STOP MERGES t_merge_mem_legacy_composite;
    INSERT INTO t_merge_mem_legacy_composite SELECT number, tuple(number, ('{\"a0\": ' || toString(number) || '}')::JSON), [('{\"a1\": ' || toString(number) || '}')::JSON] FROM numbers(1000);
    INSERT INTO t_merge_mem_legacy_composite SELECT number, tuple(number, ('{\"b0\": ' || toString(number) || '}')::JSON), [('{\"b1\": ' || toString(number) || '}')::JSON] FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_legacy_composite SELECT number, tuple(number, ('{\"c0\": ' || toString(number) || '}')::JSON), [('{\"c1\": ' || toString(number) || '}')::JSON] FROM numbers(2000, 1000);
"

# Turn the first part (all_1_1_0) into a legacy part by dropping its columns_substreams.txt, so the reservation
# estimate has to fall back to its on-disk stream count and run the unrecorded-dynamic-streams recovery.
legacy_part=$(find "${DB_PATH}" -type d -name 'all_1_1_0' | head -1)
rm -f "${legacy_part}/columns_substreams.txt"

# Second run: an explicit OPTIMIZE ... FINAL reserves memory unconditionally, so under the tiny soft limit it must
# still merge everything (including the legacy part with composite JSON columns) down to a single part and must not error.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    OPTIMIZE TABLE t_merge_mem_legacy_composite FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_legacy_composite;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_legacy_composite' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1

rm -rf "${WORKING_FOLDER}"
