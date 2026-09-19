#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the per-stream adaptive write buffer split. MergeTreeDataPartWriterWide::addStreams opens a stream with an
# adaptive write buffer when use_adaptive_write_buffer_for_dynamic_subcolumns is on and the substream is dynamic
# (ISerialization::isDynamicSubcolumn), even when the table has fewer than
# min_columns_to_activate_adaptive_write_buffer columns - so a JSON / Dynamic merge of a narrow table must be
# priced at 2 * adaptive_write_buffer_initial_size per dynamic substream, not at the full
# 2 * max_compress_block_size, while the non-dynamic skeleton of composites (Array(JSON) offsets, the scalar
# element of Tuple(UInt64, JSON)) keeps the full size. This drives that split through a horizontal merge, a
# vertical merge (whose per-writer column lists never activate the count-based rule), and a rebuilt projection
# over a semi-structured column. OPTIMIZE reserves unconditionally, so under a pathologically small soft limit
# everything must still merge to a single part and must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    SET enable_json_type = 1;
    SET enable_dynamic_type = 1;

    -- Horizontal merge: a narrow table where almost every stream is a dynamic substream.
    CREATE TABLE t_merge_mem_adaptive_h (k UInt64, j JSON, d Dynamic, t Tuple(a UInt64, b JSON), arr Array(JSON))
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_adaptive_h;
    INSERT INTO t_merge_mem_adaptive_h SELECT number, ('{\"p' || toString(number % 16) || '\": ' || toString(number) || '}')::JSON, number::Dynamic, tuple(number, ('{\"q' || toString(number % 8) || '\": ' || toString(number) || '}')::JSON), array(('{\"r\": ' || toString(number) || '}')::JSON) FROM numbers(1000);
    INSERT INTO t_merge_mem_adaptive_h SELECT number, ('{\"p' || toString(number % 16) || '\": \"s\"}')::JSON, toString(number)::Dynamic, tuple(number, ('{\"q' || toString(number % 8) || '\": \"s\"}')::JSON), array(('{\"r\": \"s' || toString(number % 4) || '\"}')::JSON) FROM numbers(1000, 1000);
    SYSTEM START MERGES t_merge_mem_adaptive_h;

    OPTIMIZE TABLE t_merge_mem_adaptive_h FINAL SETTINGS optimize_throw_if_noop = 1;
    SELECT count() FROM t_merge_mem_adaptive_h;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_adaptive_h' AND active;

    -- Vertical merge: the gathering stage writes one column per writer, so the count-based adaptive rule
    -- never fires there and only the dynamic substreams are adaptive.
    CREATE TABLE t_merge_mem_adaptive_v (k UInt64, j JSON, s String, d Dynamic)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0,
             enable_vertical_merge_algorithm = 1,
             vertical_merge_algorithm_min_rows_to_activate = 1,
             vertical_merge_algorithm_min_bytes_to_activate = 1,
             vertical_merge_algorithm_min_columns_to_activate = 1;

    SYSTEM STOP MERGES t_merge_mem_adaptive_v;
    INSERT INTO t_merge_mem_adaptive_v SELECT number, ('{\"a\": ' || toString(number) || '}')::JSON, repeat('x', 50), number::Dynamic FROM numbers(1000);
    INSERT INTO t_merge_mem_adaptive_v SELECT number, ('{\"b\": [' || toString(number) || ']}')::JSON, repeat('y', 50), toString(number)::Dynamic FROM numbers(1000, 1000);
    SYSTEM START MERGES t_merge_mem_adaptive_v;

    OPTIMIZE TABLE t_merge_mem_adaptive_v FINAL SETTINGS optimize_throw_if_noop = 1;
    SELECT count() FROM t_merge_mem_adaptive_v;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_adaptive_v' AND active;

    -- Rebuilt projection over a semi-structured column: the temp-part writer streams follow the same
    -- per-stream adaptive split.
    CREATE TABLE t_merge_mem_adaptive_proj
    (
        k UInt64,
        j JSON,
        PROJECTION p (SELECT j ORDER BY k)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, materialize_projections_on_insert = 0, materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_adaptive_proj;
    INSERT INTO t_merge_mem_adaptive_proj SELECT number, ('{\"a\": ' || toString(number) || '}')::JSON FROM numbers(1000);
    INSERT INTO t_merge_mem_adaptive_proj SELECT number, ('{\"b\": \"' || toString(number) || '\"}')::JSON FROM numbers(1000, 1000);
    SYSTEM START MERGES t_merge_mem_adaptive_proj;

    OPTIMIZE TABLE t_merge_mem_adaptive_proj FINAL SETTINGS optimize_throw_if_noop = 1;
    SELECT count() FROM t_merge_mem_adaptive_proj;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_adaptive_proj' AND active;
    SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_merge_mem_adaptive_proj' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1
