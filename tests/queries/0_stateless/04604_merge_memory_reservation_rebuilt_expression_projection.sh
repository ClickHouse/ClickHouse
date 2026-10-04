#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the projection REBUILD path when the projection materializes a semi-structured (JSON) value through an
# EXPRESSION rather than a bare source identifier. A projection like `SELECT identity(json) ...` produces an
# output column named `identity(json)`, which no base part records under that name, so the per-name substream
# match falls back to the default serialization (one stream) even though writeTempProjectionPart still writes one
# stream per real dynamic substream of the evaluated column. countRebuiltProjectionStreams must instead bound
# such a column by the total dynamic substreams present in the source parts, so the reservation is not undersized.
# Under a pathologically small merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves
# unconditionally, so it must still merge everything to a single part with the rebuilt projection intact, and must
# not error while estimating the memory of a merge that rebuilds a JSON projection column produced by an expression.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- min_bytes_for_wide_part = 0 forces the Wide format so the per-substream estimate path is exercised, and
    -- materialize_projections_on_merge = 1 makes the merge rebuild a projection that some parts do not have.
    CREATE TABLE t_merge_mem_rebuilt_expr
    (
        k UInt64,
        json JSON(a UInt64)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_rebuilt_expr;
    -- Several distinct JSON paths per part so the base column has real dynamic substreams to price against.
    INSERT INTO t_merge_mem_rebuilt_expr SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(1000);
    INSERT INTO t_merge_mem_rebuilt_expr SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_rebuilt_expr SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(2000, 1000);

    -- Add the projection AFTER the parts exist, so none of them has it materialized. Its JSON column is produced
    -- by the expression identity(json), so its output name (identity(json)) does not match the base column json,
    -- exercising the expression / renamed semi-structured branch of countRebuiltProjectionStreams.
    ALTER TABLE t_merge_mem_rebuilt_expr ADD PROJECTION p_expr (SELECT k, identity(json) ORDER BY k);

    SYSTEM START MERGES t_merge_mem_rebuilt_expr;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_rebuilt_expr FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_rebuilt_expr;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_rebuilt_expr' AND active;
    -- The rebuilt projection must be present in the single merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_rebuilt_expr' AND active
        ORDER BY name;
    -- And the base table must still answer queries correctly after the merge.
    SELECT sum(json.a) FROM t_merge_mem_rebuilt_expr;
" -- --merges_mutations_memory_usage_soft_limit=1
