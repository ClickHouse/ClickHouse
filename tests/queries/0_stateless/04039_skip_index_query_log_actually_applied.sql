-- Regression test: query_log.skip_indices should only list skip indices that actually
-- dropped at least one granule, not all indices that were in useful_indices.
--
-- Before the fix, all useful_indices were logged regardless of whether they
-- filtered out any parts
-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS t_skip_applied;

-- idx_a is minmax (priority 1 in per_part_index_orders sort) so it always runs before
-- idx_b (set, priority 2), regardless of per-part file sizes. This makes the ordering deterministic.
-- we are using ORDER BY tuple() because we don't want a primary key that could filter rows
-- before we try with skip indices
-- we use GRANULARITY 1 so that we have exactly one skip index value on each part
-- we also want to use mismatched index types since different index types have different priority
-- when we are choosing indices to filter parts with
-- in the past I saw that with matching index types, the first index was always being picked because
-- they shared the same priortiy, and the part sizes are identical
CREATE TABLE t_skip_applied
(
    a UInt64,
    b UInt64,
    INDEX idx_a a TYPE minmax GRANULARITY 1,
    INDEX idx_b b TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;

-- we want to intentionally create multiple parts
SYSTEM STOP MERGES t_skip_applied;

-- Part 1: a in [0, 9], b in {10..19} — idx_a (minmax) passes for a=5 since 5 is in [0,9], idx_b (set) filters (5 is not in {10..19})
INSERT INTO t_skip_applied SELECT number, 10 + number FROM numbers(10);
-- Part 2: a in [100, 109], b in {0..9} — idx_a (minmax) filters (5 not in [100,109]), idx_b never runs
INSERT INTO t_skip_applied SELECT 100 + number, number FROM numbers(10);

-- Both idx_a and idx_b appear: idx_a dropped part2's granule, idx_b dropped part1's granule.
-- use_skip_indexes_on_data_read=0 forces mark-selection path so indices actually drop granules here.
SELECT count() FROM t_skip_applied WHERE a = 5 AND b = 5 SETTINGS log_queries = 1, use_skip_indexes_on_data_read = 0;

SYSTEM FLUSH LOGS;

SELECT
    has(skip_indices, concat(currentDatabase(), '.t_skip_applied.idx_a')) AS idx_a_logged,
    has(skip_indices, concat(currentDatabase(), '.t_skip_applied.idx_b')) AS idx_b_logged
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%t_skip_applied%a = 5 AND b = 5%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Only idx_a appears: idx_a (minmax, always runs first due to priority) filters all granules for both parts
-- (50 not in [0,9] and 50 not in [100,109]), so idx_b never gets to run and is NOT logged.
SELECT count() FROM t_skip_applied WHERE a = 50 AND b = 50 SETTINGS log_queries = 1, use_skip_indexes_on_data_read = 0;

SYSTEM FLUSH LOGS;

SELECT
    has(skip_indices, concat(currentDatabase(), '.t_skip_applied.idx_a')) AS idx_a_logged,
    has(skip_indices, concat(currentDatabase(), '.t_skip_applied.idx_b')) AS idx_b_logged
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%t_skip_applied%a = 50 AND b = 50%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM START MERGES t_skip_applied;
DROP TABLE t_skip_applied;
