-- Tags: no-random-merge-tree-settings
--
-- A background merge runs under its own memory tracker (a child of the global "Background process
-- (mutate/merge)" tracker), not under the tracker of the query that triggered it. As a result the
-- per-query `max_memory_usage` limit does NOT bound a merge: a merge of parts holding large values
-- can consume an arbitrary amount of memory regardless of how small `max_memory_usage` is.
--
-- This is the mechanism behind the AST-fuzzer OOM in
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=107389&sha=ec12cb3ce0a49a403226cb0668b092f02a2fa3f6&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%2C%20targeted%2C%20old_compatibility%29
-- where sequential, individually-small fuzzer queries left behind parts that the background merge
-- subsystem then combined, growing server memory across query boundaries until the kernel OOM-killer
-- reaped the server (the per-query limit never sees this memory).
--
-- Related: https://github.com/ClickHouse/ClickHouse/pull/107389

DROP TABLE IF EXISTS t_merge_memory;

CREATE TABLE t_merge_memory (id UInt64, arr Array(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    -- Force a horizontal merge, the algorithm that opens all column streams at once.
    vertical_merge_algorithm_min_rows_to_activate = 1000000000,
    vertical_merge_algorithm_min_columns_to_activate = 1000000000,
    merge_max_block_size = 8192;

-- Two parts, each row carrying a ~400 KiB array. The data inserts cheaply, but merging the parts
-- opens the column write streams and buffers needed to combine them, well above the limit below.
INSERT INTO t_merge_memory SELECT number, range(50000) FROM numbers(2000);
INSERT INTO t_merge_memory SELECT number, range(50000) FROM numbers(2000);

-- A per-query memory limit that no merge of this data could ever satisfy.
-- It bounds only the `OPTIMIZE` statement's own (tiny) bookkeeping, not the merge it schedules.
SET max_memory_usage = 20000000;

-- The merge ignores `max_memory_usage` and completes successfully.
OPTIMIZE TABLE t_merge_memory FINAL;

-- One part remains: the merge ran to completion despite the 20 MB query limit.
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_merge_memory' AND active;

SYSTEM FLUSH LOGS part_log;

-- The merge's peak memory is far above the 20 MB per-query limit, proving the merge is not bounded
-- by `max_memory_usage`. (Observed ~110 MiB; the assertion uses a generous margin to stay stable
-- across builds and architectures.)
SELECT max(peak_memory_usage) > 20000000
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_memory' AND event_type = 'MergeParts';

DROP TABLE t_merge_memory;
