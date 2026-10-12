-- A `LIMIT` that must read till the end (`WITH TOTALS`, `exact_rows_before_limit`) does not exempt the
-- sort it feeds from the primary-key selectivity guard (see `limitReadsTillEnd`). That verdict must be
-- scoped to the sort's own query block: an enclosing `GROUP BY ... WITH TOTALS LIMIT 1` reads all the
-- rows the subquery produces, but the subquery's own `LIMIT 10` still lets the inner read-in-order stop
-- early, so the inner top-N plan must keep the exemption.

DROP TABLE IF EXISTS rio_pk_selectivity_nested_totals;

CREATE TABLE rio_pk_selectivity_nested_totals (path String, key UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES rio_pk_selectivity_nested_totals;

INSERT INTO rio_pk_selectivity_nested_totals SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO rio_pk_selectivity_nested_totals SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(25000, 25000);
INSERT INTO rio_pk_selectivity_nested_totals SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(50000, 25000);
INSERT INTO rio_pk_selectivity_nested_totals SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(75000, 25000);

-- The read-in-order switches are randomized by the test runner, and the guard only fires when the
-- parallel read has more than one stream to recover, so pin them.
SET max_threads = 4, enable_parallel_replicas = 0, read_in_order_use_virtual_row = 1,
    optimize_read_in_order = 1;

SELECT 'inner LIMIT below an outer WITH TOTALS LIMIT keeps read-in-order';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT count()
    FROM
    (
        SELECT path FROM rio_pk_selectivity_nested_totals
        WHERE path LIKE '%file.log'
        ORDER BY path
        LIMIT 10
    )
    GROUP BY ()
    WITH TOTALS
    LIMIT 1
    SETTINGS read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- Control: `exact_rows_before_limit` marks the inner `LIMIT 10` itself as read-till-end, so the
-- verdict taken from the sort's own `LIMIT` makes the guard fire and the read falls back to a
-- parallel scan with a full sort.
SELECT 'inner LIMIT that reads till the end falls back to a parallel read';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT count()
    FROM
    (
        SELECT path FROM rio_pk_selectivity_nested_totals
        WHERE path LIKE '%file.log'
        ORDER BY path
        LIMIT 10
    )
    GROUP BY ()
    WITH TOTALS
    LIMIT 1
    SETTINGS read_in_order_max_primary_key_ratio = 0.5, exact_rows_before_limit = 1
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'result';
SELECT count()
FROM
(
    SELECT path FROM rio_pk_selectivity_nested_totals
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10
)
GROUP BY ()
WITH TOTALS
LIMIT 1
SETTINGS read_in_order_max_primary_key_ratio = 0.5;

DROP TABLE rio_pk_selectivity_nested_totals;
