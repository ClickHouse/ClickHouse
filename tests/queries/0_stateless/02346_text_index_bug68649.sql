-- A `__text_index_*` virtual column has no stored size, and the PREWHERE score used to fall back to a
-- row count for such columns. A row count is not comparable with bytes per rejected row, so every
-- physical-column predicate won and ran first, on all rows.

SET allow_experimental_full_text_index = 1;
SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy'; -- the default plan printer reverses the AND arguments
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1; -- CI may inject False, leaving no PREWHERE to inspect
SET allow_reorder_prewhere_conditions = 1;
SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

-- Wide parts give the physical columns a real size; the tdigest on `v` activates the estimator.
CREATE TABLE tab
(
    id UInt64,
    v UInt64 STATISTICS(tdigest),
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO tab
SELECT
    number,
    number % 1000,
    concat('alpha beta gamma ', toString(number), ' ', repeat(hex(sipHash64(number)), 8))
FROM numbers(20000)
SETTINGS max_insert_threads = 1;

-- `hasToken` uses exact direct read, so it becomes a condition on the virtual column alone; the
-- equality has a non-constant right side, so it stays a raw predicate over `text`.

SELECT '-- no row matches the equality';
SELECT count() FROM tab WHERE hasToken(text, 'alpha') AND text = concat('x', toString(v));

-- The AND arguments of `Prewhere filter column` are printed in scheduling order.

SELECT '-- the residual predicate on `text` costs far more per row than the index lookup';
SELECT multiIf(
        position(explain, '__text_index') = 0 OR position(explain, 'equals(') = 0, 'unexpected plan: ' || explain,
        position(explain, '__text_index') < position(explain, 'equals('), 'index condition first',
        'physical condition first')
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasToken(text, 'alpha') AND text = concat('x', toString(v)))
WHERE explain LIKE '%Prewhere filter column%';

SELECT '-- a selective predicate on a narrow column costs less, so it goes first';
SELECT multiIf(
        position(explain, '__text_index') = 0 OR position(explain, 'equals(') = 0, 'unexpected plan: ' || explain,
        position(explain, '__text_index') < position(explain, 'equals('), 'index condition first',
        'physical condition first')
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasToken(text, 'alpha') AND v = 42)
WHERE explain LIKE '%Prewhere filter column%';

-- `_partition_id` is synthesized from part metadata, so it reads nothing and must come first even
-- against the cheapest physical column. Its type carries no fixed size, so a type-based estimate
-- would charge it more than `v` and schedule the free condition last.

SELECT '-- rows of partition `all` with v = 42';
SELECT count() FROM tab WHERE _partition_id = 'all' AND v = 42;

SELECT '-- a virtual column that reads nothing goes first';
SELECT multiIf(
        position(explain, '_partition_id') = 0 OR position(explain, '42_UInt8') = 0, 'unexpected plan: ' || explain,
        position(explain, '_partition_id') < position(explain, '42_UInt8'), 'partition condition first',
        'physical condition first')
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE _partition_id = 'all' AND v = 42)
WHERE explain LIKE '%Prewhere filter column%';

DROP TABLE tab;
