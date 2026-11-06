#!/usr/bin/env bash
# Tags: no-fasttest
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge):
# the data-volume cap on the output write buffers must bound the produced compressed output by the input UNCOMPRESSED
# volume, not by the sum of the source parts' COMPRESSED sizes. A merge interleaves rows from several parts, so parts
# that each compressed extremely well on their own (here: every part is a long run of one constant string spread over
# interleaving primary keys, so on disk it is almost nothing) merge into an alternating row order that compresses far
# worse - the multipart writers keep close to the merged output size alive in their upload buffers, which can be much
# larger than 2 * sum_input_bytes_compressed. Reserving from the compressed input volume would clamp the reservation
# far below what the writer may allocate; the estimate now uses the uncompressed volume, a sound upper bound on the
# produced compressed output. Under a pathologically small merges_mutations_memory_usage_soft_limit an explicit
# OPTIMIZE ... FINAL reserves unconditionally, so it must still merge everything down to a single part and must not
# error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- min_bytes_for_wide_part = 0 forces every part (and the merged part) to be Wide, so the output write-buffer
    -- estimate runs per column stream instead of collapsing to a single shared compact stream.
    CREATE TABLE t_merge_mem_interleaved
    (
        k UInt64,
        s String
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_interleaved;

    -- Three parts, each a single long run of one constant value spread over interleaving primary keys
    -- (k % 3 == 0 / 1 / 2). On its own every part compresses to almost nothing; the merged, k-sorted part
    -- alternates the three constants row by row and loses those runs, so the produced output is much larger
    -- (compressed) than the sum of the source parts' compressed sizes.
    INSERT INTO t_merge_mem_interleaved SELECT number * 3,     repeat('A', 256) FROM numbers(1000);
    INSERT INTO t_merge_mem_interleaved SELECT number * 3 + 1, repeat('B', 256) FROM numbers(1000);
    INSERT INTO t_merge_mem_interleaved SELECT number * 3 + 2, repeat('C', 256) FROM numbers(1000);

    SYSTEM START MERGES t_merge_mem_interleaved;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_interleaved FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_interleaved;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_interleaved' AND active;
    -- The merged part is Wide, so the output write-buffer estimate (the uncompressed-input cap) ran during selection.
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_interleaved' AND active;
    -- The data survived the merge intact.
    SELECT sum(k), uniqExact(s) FROM t_merge_mem_interleaved;
" -- --merges_mutations_memory_usage_soft_limit=1
