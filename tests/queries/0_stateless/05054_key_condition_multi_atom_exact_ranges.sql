-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: the read_rows checks depend on the index
-- granularity and on the exact-ranges optimization being applicable.

SET explain_query_plan_default = 'legacy';

-- The relaxed atoms on `toYYYYMM(ts)` and `toDate(ts)` are covered by the exact atom on `ts`
-- within the same condition, so they must not disable the exact-ranges optimization: `count()`
-- is answered from the index for the granules that match entirely, and only the boundary
-- granules are read.

DROP TABLE IF EXISTS test_exact_ranges_covered;

CREATE TABLE test_exact_ranges_covered (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY (toYYYYMM(ts), toDate(ts), ts)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_exact_ranges_covered SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24 * 40);

SELECT count() FROM test_exact_ranges_covered WHERE ts >= toDateTime('2026-01-20 00:00:00', 'UTC') SETTINGS log_comment = '05054 covered relaxed atoms';

SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 100 FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '05054 covered relaxed atoms' AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE test_exact_ranges_covered;

-- A key column that is a non-injective function of the predicate column: `x = 257` gets the
-- exact atom on `x` and the relaxed atom `toUInt8(x) = 1`. Both prune, and the exact atom keeps
-- the matched range exact, so the count reads almost nothing.

DROP TABLE IF EXISTS test_exact_ranges_cross_key;

CREATE TABLE test_exact_ranges_cross_key (x UInt16) ENGINE = MergeTree
ORDER BY (toUInt8(x), x)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_exact_ranges_cross_key SELECT number FROM numbers(1000);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_exact_ranges_cross_key WHERE x = 257) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_exact_ranges_cross_key WHERE x = 257 SETTINGS log_comment = '05054 cross key';

SYSTEM FLUSH LOGS query_log;
SELECT read_rows <= 2 FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '05054 cross key' AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE test_exact_ranges_cross_key;

-- A condition with a relaxed atom and no exact sibling must stay relaxed: the granules selected
-- by `toDate(ts) >= toDate(X)` over-approximate a mid-day bound, so counting them from the index
-- without reading would be wrong. The correct result proves the exact-ranges optimization stayed
-- disabled here.

DROP TABLE IF EXISTS test_exact_ranges_uncovered;

CREATE TABLE test_exact_ranges_uncovered (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY toDate(ts)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_exact_ranges_uncovered SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24 * 40);

SELECT count() FROM test_exact_ranges_uncovered WHERE ts >= toDateTime('2026-01-20 12:00:00', 'UTC');
SELECT count() FROM test_exact_ranges_uncovered WHERE ts >= toDateTime('2026-01-20 12:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_exact_ranges_uncovered;
