-- Ported from DuckDB test/sql/join/iejoin/test_iesemijoin.test and test_ieantijoin.test.
-- DuckDB executes SEMI/ANTI/LEFT/RIGHT/FULL inequality joins with IEJoin; ClickHouse does not
-- support these join kinds with an inequality-only ON section yet, so this test checks the
-- INNER part and locks the current error for the rest (to be revisited with IEJoin for other kinds).

SET allow_experimental_ie_join = 1;

DROP TABLE IF EXISTS left_small;
DROP TABLE IF EXISTS right_small;

CREATE TABLE left_small (id Int32, start Nullable(Date), stop Nullable(Date), symbol String, price Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE right_small (id Int32, start Nullable(Date), stop Nullable(Date), symbol String, bid Float64, active Bool) ENGINE = MergeTree ORDER BY id;

INSERT INTO left_small VALUES
    (1, '2026-01-01', '2026-01-02', 'A', 150.00),
    (2, '2026-01-02', '2026-01-03', 'A', 151.00),
    (3, '2026-01-03', '2026-01-04', 'B', 380.00),
    (4, '2026-01-05', NULL, 'C', 410.0),
    (5, NULL, '2026-01-06', 'C', 420.0);

INSERT INTO right_small VALUES
    (1, '2026-01-01', '2026-01-03', 'A', 149.50, true),
    (2, '2026-01-03', '2026-01-04', 'A', 150.50, false),
    (3, '2026-01-04', '2026-01-05', 'B', 379.00, true);

-- The INNER join works via IEJoin; the rows with NULL keys (ids 4 and 5) never match
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id, r.id FROM left_small l JOIN right_small r ON l.start < r.stop AND r.start < l.stop) WHERE explain LIKE '%IEJoin%';
SELECT l.id, r.id FROM left_small l JOIN right_small r ON l.start < r.stop AND r.start < l.stop ORDER BY ALL;

SELECT l.id FROM left_small l LEFT SEMI JOIN right_small r ON l.start < r.stop AND r.start < l.stop; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT l.id FROM left_small l LEFT ANTI JOIN right_small r ON l.start < r.stop AND r.start < l.stop; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT l.id, r.id FROM left_small l LEFT JOIN right_small r ON l.start < r.stop AND r.start < l.stop; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT l.id, r.id FROM left_small l RIGHT JOIN right_small r ON l.start < r.stop AND r.start < l.stop; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT l.id, r.id FROM left_small l FULL JOIN right_small r ON l.start < r.stop AND r.start < l.stop; -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE left_small;
DROP TABLE right_small;
