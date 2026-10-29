-- Tags: no-fasttest
-- no-fasttest - collations support is disabled for fasttest build

-- Under a ROWS frame, `rank` and `dense_rank` must treat values equal under the ORDER BY collation
-- as peers, even though such values differ in bytes and may interleave in the sorted input.

-- The second key forces the order a, A, a, so equal raw values are not adjacent.
SELECT 'interleaved', arraySort(groupArray((v, k, r))) FROM (SELECT v, k, RANK() OVER (ORDER BY v COLLATE 'en-u-ks-level2', k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin([('a', 1), ('A', 2), ('a', 3), ('b', 4)]) AS t, t.1 AS v, t.2 AS k));
SELECT 'interleaved peers', arraySort(groupArray((v, k, r))) FROM (SELECT v, k, RANK() OVER (ORDER BY v COLLATE 'en-u-ks-level2', k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin([('a', 1), ('A', 1), ('a', 1), ('b', 2)]) AS t, t.1 AS v, t.2 AS k));

SELECT 'rank', arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v COLLATE 'en-u-ks-level2' ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin(['a', 'A', 'a', 'b', 'B']) AS v));
SELECT 'dense_rank', arraySort(groupArray((v, r))) FROM (SELECT v, DENSE_RANK() OVER (ORDER BY v COLLATE 'en-u-ks-level2' ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin(['a', 'A', 'a', 'b', 'B']) AS v));
SELECT 'numeric', arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v COLLATE 'en-u-kn-true' ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin(['1', '01', '2', '10']) AS v));

SELECT 'many rows', count(), min(r), max(r), min(d), max(d) FROM (SELECT RANK() OVER (ORDER BY v COLLATE 'en-u-ks-level2' ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r, DENSE_RANK() OVER (ORDER BY v COLLATE 'en-u-ks-level2' ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS d FROM (SELECT if(number % 2, 'a', 'A') AS v FROM numbers(1000)));
SELECT 'descending', count(), min(r), max(r) FROM (SELECT RANK() OVER (ORDER BY v DESC COLLATE 'en-u-ks-level2' ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r FROM (SELECT if(number % 3 = 0, 'b', if(number % 2, 'a', 'A')) AS v FROM numbers(999)));

SELECT 'nullable', arraySort(groupArray((v, r))) FROM (SELECT v, DENSE_RANK() OVER (ORDER BY v COLLATE 'en-u-ks-level2' ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT toNullable(arrayJoin(['a', 'A', NULL, 'b', NULL])) AS v));
SELECT 'low_cardinality', arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v COLLATE 'en-u-ks-level2' ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT toLowCardinality(arrayJoin(['a', 'A', 'a', 'b'])) AS v));

-- One row per block, so peers are compared against the last row of the previous block.
SELECT 'cross_block', arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v COLLATE 'en-u-ks-level2' ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin(['a', 'A', 'a', 'b', 'B', 'c']) AS v)) SETTINGS max_block_size = 1;
SELECT 'partition', arraySort(groupArray((p, v, r))) FROM (SELECT p, v, RANK() OVER (PARTITION BY p ORDER BY v COLLATE 'en-u-ks-level2' ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin([(1, 'a'), (1, 'A'), (1, 'b'), (2, 'A'), (2, 'b'), (2, 'B')]) AS t, t.1 AS p, t.2 AS v)) SETTINGS max_block_size = 1;
