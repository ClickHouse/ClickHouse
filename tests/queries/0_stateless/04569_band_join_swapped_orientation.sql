-- Tags: no-old-analyzer

-- The band shape with the point expression on the query's right table: detection accepts it,
-- and `BandJoinStep` executes it as the left-point mirror — the input pipelines are swapped
-- (`Swapped: true`, `PointSide: Right` in EXPLAIN) and the query column order is restored on
-- top of the join. Results are verified against a brute-force cross-join oracle and
-- byte-for-byte against `ie_join` on the same queries.

-- Pin the written join order: the join order optimizer may flip the inputs on its own, which
-- would change which orientation reaches the band join planner code.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS swo_p;
DROP TABLE IF EXISTS swo_i;

CREATE TABLE swo_p (id UInt32, t Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE swo_i (id UInt32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;

-- Small domain for heavy key duplication; interval widths from -2 (empty) to 4.
INSERT INTO swo_p SELECT number, ((number * number + 6789) % 2147483647) % 40 FROM numbers(400);
INSERT INTO swo_i
    SELECT number, x, x + (number % 7) - 2
    FROM (SELECT number, (((number + 100) * (number + 100) + 12345) % 2147483647) % 40 AS x FROM numbers(400));

-- The interval table written on the left: the shared point expression comes from the right
SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%BandJoin%';
SELECT 'point side line', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%PointSide: Right%';
SELECT 'swapped line', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%Swapped: true%';

-- The left orientation stays un-swapped
SELECT 'left not swapped', count() FROM (EXPLAIN actions = 1 SELECT count() FROM swo_p p JOIN swo_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%Swapped%';

-- The restored column order: `SELECT *` carries the interval table's columns first
SELECT 'column order';
SELECT * FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t <= i.hi ORDER BY ALL LIMIT 3;

-- All four strict/loose bracket combinations vs the oracle and `ie_join`, full rows so the
-- restored order is covered too
SELECT '>= <=' AS brackets,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i, swo_p p WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count() FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

SELECT '> <=' AS brackets,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i, swo_p p WHERE p.t > i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t > i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count() FROM swo_i i JOIN swo_p p ON p.t > i.lo AND p.t <= i.hi) AS cnt;

SELECT '>= <' AS brackets,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i, swo_p p WHERE p.t >= i.lo AND p.t < i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count() FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t < i.hi) AS cnt;

SELECT '> <' AS brackets,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i, swo_p p WHERE p.t > i.lo AND p.t < i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM swo_i i JOIN swo_p p ON p.t > i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count() FROM swo_i i JOIN swo_p p ON p.t > i.lo AND p.t < i.hi) AS cnt;

-- The reversed spelling of the bounds and `BETWEEN` detect in the swapped orientation too
SELECT 'reversed spelling', count() > 0 FROM (EXPLAIN SELECT count() FROM swo_i i JOIN swo_p p ON i.lo <= p.t AND i.hi >= p.t) WHERE explain LIKE '%BandJoin%';
SELECT 'between',
    (SELECT arraySort(groupArray((i.id, p.id))) FROM swo_i i JOIN swo_p p ON p.t BETWEEN i.lo AND i.hi)
        = (SELECT arraySort(groupArray((i.id, p.id))) FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t <= i.hi);

-- Both orientations produce the same match set
SELECT 'orientation parity',
    (SELECT arraySort(groupArray((p.id, i.id))) FROM swo_i i JOIN swo_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM swo_p p JOIN swo_i i ON p.t >= i.lo AND p.t <= i.hi);

DROP TABLE swo_p;
DROP TABLE swo_i;
