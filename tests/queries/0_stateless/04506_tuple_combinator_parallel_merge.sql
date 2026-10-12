-- `uniqExact` merges its states through a thread pool, and the `-Tuple` combinator forwards the
-- capability element-wise. `max_threads` is pinned above 1 so that several partial aggregation
-- states exist and the final merge actually takes the parallel path; the results must match the
-- plain functions. An only-null element resolves to a placeholder without parallel merge support
-- and merges plainly alongside the parallelized element.
SET max_threads = 4;

SELECT 'parallel merge equals plain';
SELECT (SELECT uniqExactTuple((number, intDiv(number, 2))) FROM numbers_mt(300000)) = (SELECT (uniqExact(number), uniqExact(intDiv(number, 2))) FROM numbers_mt(300000));

SELECT 'placeholder element alongside a parallelized element';
SELECT uniqExactTuple((NULL, number)) FROM numbers_mt(300000);

SELECT 'keyed merge equals plain';
SELECT (SELECT groupArray((k, tupleElement(r, 1))) FROM (SELECT number % 4 AS k, uniqExactTuple(tuple(number)) AS r FROM numbers_mt(300000) GROUP BY k ORDER BY k))
     = (SELECT groupArray((k, u)) FROM (SELECT number % 4 AS k, uniqExact(number) AS u FROM numbers_mt(300000) GROUP BY k ORDER BY k));

-- External aggregation spills every block and merges the deserialized states back through the
-- batched keyed merge, which -Tuple forwards to the nested functions element-wise.
SELECT 'external aggregation merge equals plain';
SELECT (SELECT groupArray((k, tupleElement(r, 1))) FROM (SELECT number % 4 AS k, uniqExactTuple(tuple(number)) AS r FROM numbers_mt(300000) GROUP BY k ORDER BY k SETTINGS max_bytes_before_external_group_by = 1))
     = (SELECT groupArray((k, u)) FROM (SELECT number % 4 AS k, uniqExact(number) AS u FROM numbers_mt(300000) GROUP BY k ORDER BY k));
SELECT (SELECT groupArray((k, r)) FROM (SELECT number % 4 AS k, sumTuple((number, number * 2)) AS r FROM numbers_mt(300000) GROUP BY k ORDER BY k SETTINGS max_bytes_before_external_group_by = 1))
     = (SELECT groupArray((k, (s1, s2))) FROM (SELECT number % 4 AS k, sum(number) AS s1, sum(number * 2) AS s2 FROM numbers_mt(300000) GROUP BY k ORDER BY k));
