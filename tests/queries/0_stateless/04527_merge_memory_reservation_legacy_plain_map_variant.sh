#!/usr/bin/env bash
# Regression test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / countOutputStreams) on a legacy wide part (one that predates columns_substreams.txt) that mixes columns with a
# dynamic structure (JSON / Dynamic) with plain Map / Variant columns. The unrecorded-dynamic-streams recovery only
# runs for such a legacy part, and it must count only the streams that the default serialization cannot enumerate,
# i.e. the columns with hasDynamicStructure(). A plain Map or Variant reports hasDynamicSubcolumns() == true but
# hasDynamicStructure() == false: its physical streams are fully enumerable and are already counted once by the
# per-column union, so keying the recovery off hasDynamicSubcolumns() would count them a second time and over-reserve
# the merge on the upgrade path. This test exercises exactly that mix and checks the merge still succeeds.
#
# A pre-25.8 legacy wide part is simulated by deleting columns_substreams.txt from an existing part on disk (via a
# persistent clickhouse-local --path so the two runs share the data directory). Under a pathologically small
# merges_mutations_memory_usage_soft_limit the explicit OPTIMIZE ... FINAL must still reduce everything to one part
# and must not error while estimating a merge of a legacy part holding both a JSON column and plain Map / Variant.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04527_merge_memory_reservation_legacy_plain_map_variant"
DB_PATH="${WORKING_FOLDER}/db"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# First run: create three wide parts, each with a JSON column carrying disjoint paths and plain Map / Variant
# columns. The large merge_selecting_sleep_ms keeps background merge selection from firing before the explicit
# OPTIMIZE below, while STOP MERGES keeps the three inserts as separate parts. min_bytes_for_wide_part = 0 forces
# the Wide format so the per-substream estimate path is exercised.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    SET enable_json_type = 1;
    SET enable_variant_type = 1;

    CREATE TABLE t_merge_mem_legacy_map (k UInt64, m Map(String, String), v Variant(UInt64, String), j JSON)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, merge_selecting_sleep_ms = 600000, max_merge_selecting_sleep_ms = 600000;

    SYSTEM STOP MERGES t_merge_mem_legacy_map;
    INSERT INTO t_merge_mem_legacy_map SELECT number, map('a', toString(number)), number::Variant(UInt64, String), ('{\"a0\": ' || toString(number) || '}')::JSON FROM numbers(1000);
    INSERT INTO t_merge_mem_legacy_map SELECT number, map('b', toString(number)), toString(number)::Variant(UInt64, String), ('{\"b0\": ' || toString(number) || '}')::JSON FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_legacy_map SELECT number, map('c', toString(number)), number::Variant(UInt64, String), ('{\"c0\": ' || toString(number) || '}')::JSON FROM numbers(2000, 1000);
"

# Turn the first part (all_1_1_0) into a legacy part by dropping its columns_substreams.txt, so the reservation
# estimate has to fall back to its on-disk stream count and run the unrecorded-dynamic-streams recovery.
legacy_part=$(find "${DB_PATH}" -type d -name 'all_1_1_0' | head -1)
rm -f "${legacy_part}/columns_substreams.txt"

# Second run: an explicit OPTIMIZE ... FINAL reserves memory unconditionally, so under the tiny soft limit it must
# still merge everything (including the legacy part with plain Map / Variant) down to a single part and must not error.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    OPTIMIZE TABLE t_merge_mem_legacy_map FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_legacy_map;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_legacy_map' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1

rm -rf "${WORKING_FOLDER}"
