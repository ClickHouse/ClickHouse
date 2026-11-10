-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/121912
--
-- A filter that combines a condition on a column carrying a `set` skip index with
-- `coalesce(<expr>, <expr>, <const>) = <const>` over columns that index does not cover used to
-- fail index analysis with `NOT_FOUND_COLUMN_IN_BLOCK` once
-- `allow_key_condition_coalesce_rewrite` was enabled. Both arms of that setting must return the
-- same count, and the index must still be analysed.

DROP TABLE IF EXISTS t_05255;

CREATE TABLE t_05255
(
    c1 String,
    c2 String,
    c3 Nullable(String),
    INDEX i1 c1 TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY c1
SETTINGS index_granularity = 8192;

INSERT INTO t_05255
SELECT ['e1','e2','e3'][number % 3 + 1],
       if(number % 2 = 0, 'v1', ''),
       if(number % 5 = 0, NULL, ['v1','v2',''][number % 3 + 1])
FROM numbers(100000);

-- The two arms of the setting must agree.
SELECT count() FROM t_05255
WHERE c1 = 'e1' AND coalesce(nullIf(c2, ''), nullIf(c3, ''), 'x') = 'v1'
SETTINGS allow_key_condition_coalesce_rewrite = 1, use_skip_indexes = 1;

SELECT count() FROM t_05255
WHERE c1 = 'e1' AND coalesce(nullIf(c2, ''), nullIf(c3, ''), 'x') = 'v1'
SETTINGS allow_key_condition_coalesce_rewrite = 0, use_skip_indexes = 1;

-- The `ifNull` spelling of the same rewrite, over a single uncovered column.
SELECT count() FROM t_05255
WHERE c1 = 'e1' AND ifNull(nullIf(c2, ''), 'x') = 'v1'
SETTINGS allow_key_condition_coalesce_rewrite = 1, use_skip_indexes = 1;

SELECT count() FROM t_05255
WHERE c1 = 'e1' AND ifNull(nullIf(c2, ''), 'x') = 'v1'
SETTINGS allow_key_condition_coalesce_rewrite = 0, use_skip_indexes = 1;

-- Analysis alone used to throw, and `i1` must still be analysed here: were it skipped, the
-- queries above would pass while pinning nothing. Anchor the pattern on the index-name line,
-- because the surrounding database name can itself contain `i1`.
SELECT count() > 0 FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_05255
    WHERE c1 = 'e1' AND coalesce(nullIf(c2, ''), nullIf(c3, ''), 'x') = 'v1'
    SETTINGS allow_key_condition_coalesce_rewrite = 1, use_skip_indexes = 1
)
WHERE explain ILIKE '%Name: i1%';

DROP TABLE t_05255;
