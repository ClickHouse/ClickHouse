-- Tags: no-old-analyzer

-- SEMI/ANTI/LEFT/RIGHT/FULL joins with two inequality conditions route through IEJoin;
-- extra conjuncts become a residual condition inside the operator; an OR of inequalities
-- stays error-locked. `join_use_nulls` is enabled so that unmatched rows are padded with
-- NULLs; the rows with NULL keys (ids 4 and 5) never match but are emitted as unmatched,
-- not dropped.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET join_use_nulls = 1;

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

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id, r.id FROM left_small l JOIN right_small r ON l.start < r.stop AND r.start < l.stop) WHERE explain LIKE '%IEJoin%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM left_small l LEFT SEMI JOIN right_small r ON l.start < r.stop AND r.start < l.stop) WHERE explain LIKE '%IEJoin%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM left_small l LEFT ANTI JOIN right_small r ON l.start < r.stop AND r.start < l.stop) WHERE explain LIKE '%IEJoin%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id, r.id FROM left_small l LEFT JOIN right_small r ON l.start < r.stop AND r.start < l.stop) WHERE explain LIKE '%IEJoin%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id, r.id FROM left_small l RIGHT JOIN right_small r ON l.start < r.stop AND r.start < l.stop) WHERE explain LIKE '%IEJoin%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id, r.id FROM left_small l FULL JOIN right_small r ON l.start < r.stop AND r.start < l.stop) WHERE explain LIKE '%IEJoin%';

SELECT 'inner';
SELECT l.id, r.id FROM left_small l JOIN right_small r ON l.start < r.stop AND r.start < l.stop ORDER BY ALL;
SELECT 'semi';
SELECT l.id, l.start, l.stop FROM left_small l LEFT SEMI JOIN right_small r ON l.start < r.stop AND r.start < l.stop ORDER BY ALL;
SELECT 'anti';
SELECT l.id, l.start, l.stop FROM left_small l LEFT ANTI JOIN right_small r ON l.start < r.stop AND r.start < l.stop ORDER BY ALL;
SELECT 'left';
SELECT l.id, r.id FROM left_small l LEFT JOIN right_small r ON l.start < r.stop AND r.start < l.stop ORDER BY ALL;
SELECT 'right';
SELECT l.id, r.id FROM left_small l RIGHT JOIN right_small r ON l.start < r.stop AND r.start < l.stop ORDER BY ALL;
SELECT 'full';
SELECT l.id, r.id FROM left_small l FULL JOIN right_small r ON l.start < r.stop AND r.start < l.stop ORDER BY ALL;

-- An extra conjunct beyond the two inequalities affects matching for non-INNER kinds (it cannot
-- be split off into a filter over the join result), so it is evaluated inside the operator as a
-- residual condition. A disjunction of inequality conditions stays unsupported.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM left_small l LEFT SEMI JOIN right_small r ON l.start < r.stop AND r.start < l.stop AND l.price + r.bid > 300) WHERE explain LIKE '%Residual filter%';
SELECT 'semi residual';
SELECT l.id FROM left_small l LEFT SEMI JOIN right_small r ON l.start < r.stop AND r.start < l.stop AND l.price + r.bid > 300 ORDER BY ALL;
SELECT 'anti residual';
SELECT l.id FROM left_small l LEFT ANTI JOIN right_small r ON l.start < r.stop AND r.start < l.stop AND l.price + r.bid > 300 ORDER BY ALL;
SELECT 'full residual';
SELECT l.id, r.id FROM left_small l FULL JOIN right_small r ON l.start < r.stop AND r.start < l.stop AND l.price + r.bid > 300 ORDER BY ALL;
SELECT l.id FROM left_small l LEFT ANTI JOIN right_small r ON (l.start < r.stop AND r.start < l.stop) OR (l.start > r.stop AND r.start > l.stop); -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE left_small;
DROP TABLE right_small;
