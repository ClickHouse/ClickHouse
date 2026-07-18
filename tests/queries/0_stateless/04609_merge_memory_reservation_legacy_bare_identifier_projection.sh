#!/usr/bin/env bash
# Regression test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / countRebuiltProjectionStreams) on a REBUILT bare-identifier JSON projection over the mixed legacy/new upgrade
# path. A projection like SELECT json ORDER BY json.a is a bare identifier of the base json column, so a rebuild
# writes exactly that column's dynamic substreams. When a pre-columns_substreams.txt legacy wide part (whose
# dynamic paths are invisible to the recorded per-name union) is merged with a newer part that records only its
# own paths, pricing the projection from the newer part's recorded union alone would drop the legacy part's
# dynamic paths and undersize the reservation. tryCountBareIdentifierProjectionSubstreams must therefore give up
# the precise per-name union when any matching source part lacks recorded substreams and fall back to the type's
# write-time capacity, which no rebuilt column can exceed.
#
# A pre-25.8 legacy wide part is simulated by deleting columns_substreams.txt from the first part on disk (via a
# persistent clickhouse-local --path so the two runs share the data directory). Under a pathologically small
# merges_mutations_memory_usage_soft_limit the explicit OPTIMIZE ... FINAL reserves unconditionally, so it must
# still merge everything down to a single part with the rebuilt projection intact and must not error while
# estimating a merge that rebuilds a bare-identifier JSON projection over a legacy part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/04609_merge_memory_reservation_legacy_bare_identifier_projection"
DB_PATH="${WORKING_FOLDER}/db"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# First run: create three wide parts with disjoint dynamic JSON paths (b/c/d per part) plus the typed path a, so
# the base json column carries real dynamic substreams and the parts' dynamic layouts differ. STOP MERGES keeps the
# three inserts as separate parts; the large merge_selecting_sleep_ms keeps background selection from firing before
# the explicit OPTIMIZE below. min_bytes_for_wide_part = 0 forces the Wide format so the per-substream path runs.
# materialize_projections_on_merge = 1 makes the merge rebuild the projection that no part has materialized.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    SET enable_json_type = 1;

    CREATE TABLE t_merge_mem_legacy_bare (k UInt64, json JSON(a UInt64))
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, materialize_projections_on_merge = 1,
             merge_selecting_sleep_ms = 600000, max_merge_selecting_sleep_ms = 600000;

    SYSTEM STOP MERGES t_merge_mem_legacy_bare;
    INSERT INTO t_merge_mem_legacy_bare SELECT number, ('{\"a\": ' || toString(number) || ', \"b\": ' || toString(number * 2) || '}')::JSON FROM numbers(1000);
    INSERT INTO t_merge_mem_legacy_bare SELECT number, ('{\"a\": ' || toString(number) || ', \"c\": ' || toString(number * 3) || '}')::JSON FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_legacy_bare SELECT number, ('{\"a\": ' || toString(number) || ', \"d\": ' || toString(number * 4) || '}')::JSON FROM numbers(2000, 1000);

    -- Add the bare-identifier projection AFTER the parts exist, so none has it materialized and the merge rebuilds
    -- it from the merged base rows. json.a is a typed path usable as the projection sort key.
    ALTER TABLE t_merge_mem_legacy_bare ADD PROJECTION p_bare (SELECT json ORDER BY json.a);
"

# Turn the first part (all_1_1_0) into a legacy part by dropping its columns_substreams.txt, so the projection
# column has a matching source part with no recorded substreams and the bare-identifier estimate has to fall back
# to the type's write-time capacity instead of trusting the newer parts' recorded union.
legacy_part=$(find "${DB_PATH}" -type d -name 'all_1_1_0' | head -1)
rm -f "${legacy_part}/columns_substreams.txt"

# Second run: an explicit OPTIMIZE ... FINAL reserves memory unconditionally, so under the tiny soft limit it must
# still merge everything (including the legacy part) down to a single part with the rebuilt projection and not error.
${CLICKHOUSE_LOCAL} --path "${DB_PATH}" -q "
    OPTIMIZE TABLE t_merge_mem_legacy_bare FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_legacy_bare;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_legacy_bare' AND active;
    -- The rebuilt projection must be present in the single merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_legacy_bare' AND active
        ORDER BY name;
    -- And it must still answer queries correctly after the merge.
    SELECT sum(json.a) FROM t_merge_mem_legacy_bare;
" -- --merges_mutations_memory_usage_soft_limit=1

rm -rf "${WORKING_FOLDER}"
