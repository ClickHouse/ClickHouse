-- rank() and dense_rank() are defined on the ORDER BY peer group, so an explicit frame must
-- not affect them. Results are collected as sorted (key, rank) pairs so the assertions do not
-- depend on the order rows reach the aggregation.

SELECT 'issue reproducers';
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b']) AS v));
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b']) AS v));

SELECT 'no frame';
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v) AS r FROM (SELECT arrayJoin(['a', 'a', 'b']) AS v));
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, DENSE_RANK() OVER (ORDER BY v) AS r FROM (SELECT arrayJoin(['a', 'a', 'b']) AS v));

SELECT 'RANGE frame';
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b']) AS v));
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, DENSE_RANK() OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b']) AS v));

SELECT 'other ROWS frames';
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'c']) AS v));
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'c']) AS v));
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'c']) AS v));
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'c']) AS v));

SELECT 'no ORDER BY';
SELECT arraySort(groupArray(r)) FROM (SELECT RANK() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b']) AS v));
SELECT arraySort(groupArray(r)) FROM (SELECT DENSE_RANK() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b']) AS v));

SELECT 'PARTITION BY';
SELECT arraySort(groupArray((p, v, r))) FROM (SELECT p, v, RANK() OVER (PARTITION BY p ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin([(1, 'a'), (1, 'a'), (1, 'b'), (2, 'a'), (2, 'b')]) AS t, t.1 AS p, t.2 AS v));
SELECT arraySort(groupArray((p, v, r))) FROM (SELECT p, v, DENSE_RANK() OVER (PARTITION BY p ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin([(1, 'a'), (1, 'a'), (1, 'b'), (2, 'a'), (2, 'b')]) AS t, t.1 AS p, t.2 AS v));

SELECT 'multiple ORDER BY keys';
SELECT arraySort(groupArray((v, w, r))) FROM (SELECT v, w, RANK() OVER (ORDER BY v, w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin([('a', 1), ('a', 2), ('a', 2), ('b', 1), ('b', 1)]) AS t, t.1 AS v, t.2 AS w));
SELECT arraySort(groupArray((v, w, r))) FROM (SELECT v, w, DENSE_RANK() OVER (ORDER BY v, w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin([('a', 1), ('a', 2), ('a', 2), ('b', 1), ('b', 1)]) AS t, t.1 AS v, t.2 AS w));

SELECT 'DESC';
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'c', 'c', 'c']) AS v));
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, DENSE_RANK() OVER (ORDER BY v DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'c', 'c', 'c']) AS v));

SELECT 'Nullable key';
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin([1, 1, NULL, NULL, 2]) AS v));
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin([1, 1, NULL, NULL, 2]) AS v));
-- Every group of equal keys must get exactly one distinct rank, wherever NULLs sort to.
SELECT max(d) FROM (SELECT count(DISTINCT r) AS d FROM (SELECT v, RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin([1, 1, NULL, NULL, 2]) AS v)) GROUP BY v);

SELECT 'LowCardinality key';
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT toLowCardinality(arrayJoin(['a', 'a', 'b', 'b', 'c'])) AS v));
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT toLowCardinality(arrayJoin(['a', 'a', 'b', 'b', 'c'])) AS v));

SELECT 'denseRank alias';
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, denseRank() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b']) AS v));

SELECT 'frame bounds unaffected';
SELECT arraySort(groupArray((v, s))) FROM (SELECT v, sum(1) OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s FROM (SELECT arrayJoin(['a', 'a', 'b', 'c', 'c', 'c']) AS v));
SELECT arraySort(groupArray((v, s))) FROM (SELECT v, sum(1) OVER (ORDER BY v RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s FROM (SELECT arrayJoin(['a', 'a', 'b', 'c', 'c', 'c']) AS v));
SELECT arraySort(groupArray((v, s))) FROM (SELECT v, sum(1) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM (SELECT arrayJoin(['a', 'a', 'b', 'c', 'c', 'c']) AS v));

SELECT 'row_number unaffected';
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, row_number() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'c', 'c', 'c']) AS v));

SELECT 'peer group spanning blocks';
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin(['a', 'a', 'a', 'a', 'b', 'b', 'b', 'c']) AS v)) SETTINGS max_block_size = 2;
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin(['a', 'a', 'a', 'a', 'b', 'b', 'b', 'c']) AS v)) SETTINGS max_block_size = 2;
SELECT arraySort(groupArray((p, v, r))) FROM (SELECT p, v, RANK() OVER (PARTITION BY p ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM (SELECT arrayJoin([(1, 'a'), (1, 'a'), (1, 'a'), (2, 'a'), (2, 'a'), (2, 'b'), (2, 'b')]) AS t, t.1 AS p, t.2 AS v)) SETTINGS max_block_size = 2;
SELECT arraySort(groupArray(r)) FROM (SELECT RANK() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM numbers(10)) SETTINGS max_block_size = 2;
-- A frame starting after the current row lets the transform drop blocks the peer group check
-- still reads back into.
SELECT sum(r), sum(d) FROM (SELECT RANK() OVER (PARTITION BY intDiv(number, 50) ORDER BY intDiv(number, 3) ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS r, DENSE_RANK() OVER (PARTITION BY intDiv(number, 50) ORDER BY intDiv(number, 3) ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS d FROM numbers(600)) SETTINGS max_block_size = 1;
SELECT sum(r), sum(d) FROM (SELECT RANK() OVER (PARTITION BY number % 7 ORDER BY intDiv(number, 11) ROWS BETWEEN 3 FOLLOWING AND 9 FOLLOWING) AS r, DENSE_RANK() OVER (PARTITION BY number % 7 ORDER BY intDiv(number, 11) ROWS BETWEEN 3 FOLLOWING AND 9 FOLLOWING) AS d FROM numbers(600)) SETTINGS max_block_size = 2;

SELECT 'chunk split independence';
-- Forward-looking frame ends make the row loop re-run for the same row, so the ranks must not
-- depend on max_block_size. Each query below must report exactly one distinct result.
SELECT countDistinct(a) FROM (
    SELECT arraySort(groupArray(r)) AS a FROM (SELECT RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 2
    UNION ALL SELECT arraySort(groupArray(r)) AS a FROM (SELECT RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 3
    UNION ALL SELECT arraySort(groupArray(r)) AS a FROM (SELECT RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)));
SELECT countDistinct(a) FROM (
    SELECT arraySort(groupArray(r)) AS a FROM (SELECT DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 2
    UNION ALL SELECT arraySort(groupArray(r)) AS a FROM (SELECT DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 3
    UNION ALL SELECT arraySort(groupArray(r)) AS a FROM (SELECT DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)));
SELECT countDistinct(a) FROM (
    SELECT arraySort(groupArray(r)) AS a FROM (SELECT DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 2
    UNION ALL SELECT arraySort(groupArray(r)) AS a FROM (SELECT DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 3
    UNION ALL SELECT arraySort(groupArray(r)) AS a FROM (SELECT DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)));
SELECT countDistinct(a) FROM (
    SELECT arraySort(groupArray(r)) AS a FROM (SELECT DENSE_RANK() OVER (ORDER BY v RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 2
    UNION ALL SELECT arraySort(groupArray(r)) AS a FROM (SELECT DENSE_RANK() OVER (ORDER BY v RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 3
    UNION ALL SELECT arraySort(groupArray(r)) AS a FROM (SELECT DENSE_RANK() OVER (ORDER BY v RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)));
SELECT countDistinct(a) FROM (
    SELECT arraySort(groupArray(r)) AS a FROM (SELECT RANK() OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 2
    UNION ALL SELECT arraySort(groupArray(r)) AS a FROM (SELECT RANK() OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 3
    UNION ALL SELECT arraySort(groupArray(r)) AS a FROM (SELECT RANK() OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)));

SELECT 'chunk split values';
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 2;
SELECT arraySort(groupArray((v, r))) FROM (SELECT v, DENSE_RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS r FROM (SELECT arrayJoin(['a', 'a', 'b', 'b', 'b', 'c', 'd', 'd', 'e']) AS v)) SETTINGS max_block_size = 3;
