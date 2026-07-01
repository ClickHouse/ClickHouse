-- Keys that are bitwise distinct yet compare equal (-0.0 vs +0.0, NaNs with different
-- payloads) tie under ORDER BY: the heap must never evict a key tied with the boundary.

SET enable_group_by_top_k_optimization = 1;
SET max_threads = 1;
SET max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'alternating negative and positive zero, stateful aggregate, LIMIT 1';
SELECT k, uniqExact(v) FROM
(
    SELECT
        if(number % 2 = 0, toFloat64(0), reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808)))) AS k,
        number % 5 AS v
    FROM numbers(100000)
)
GROUP BY k ORDER BY k ASC LIMIT 1;

SELECT 'same result without the optimization';
SELECT k, uniqExact(v) FROM
(
    SELECT
        if(number % 2 = 0, toFloat64(0), reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808)))) AS k,
        number % 5 AS v
    FROM numbers(100000)
)
GROUP BY k ORDER BY k ASC LIMIT 1
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'alternating NaN payloads, LIMIT 1';
SELECT isNaN(k), uniqExact(v) FROM
(
    SELECT
        if(number % 2 = 0,
           reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090561))),
           reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090562)))) AS k,
        number % 5 AS v
    FROM numbers(100000)
)
GROUP BY k ORDER BY k ASC NULLS FIRST LIMIT 1;

SELECT 'zeros tied at the boundary among real eviction churn';
SELECT count(), countIf(complete) FROM
(
    SELECT l.u = f.u AS complete
    FROM
    (
        SELECT k, uniqExact(v) AS u FROM (SELECT multiIf(number % 4 = 0, toFloat64(0), number % 4 = 1, reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808))), toFloat64(-1000000 + intDiv(toInt64(number), 4))) AS k, number % 5 AS v FROM numbers(100000))
        GROUP BY k ORDER BY k DESC LIMIT 3
        SETTINGS enable_group_by_top_k_optimization = 1
    ) AS l
    INNER JOIN
    (
        SELECT k, uniqExact(v) AS u FROM (SELECT multiIf(number % 4 = 0, toFloat64(0), number % 4 = 1, reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808))), toFloat64(-1000000 + intDiv(toInt64(number), 4))) AS k, number % 5 AS v FROM numbers(100000))
        GROUP BY k
        SETTINGS enable_group_by_top_k_optimization = 0
    ) AS f ON l.k = f.k
);

-- A boundary tie-set that keeps growing must not grow the heap without bound.
-- Prefix mode (ORDER BY a, a prefix of GROUP BY a,b): early rows vary `a` so the
-- heap evicts (which disables the never-evicted freeze), then ~1.3M distinct `b`
-- all share the in-heap boundary prefix `a = 0` and tie, so the heap would grow
-- forever without the `tie_overflow` cap.  It must freeze instead; results stay
-- correct (prefix mode never erases) and peak memory stays bounded.
SELECT 'tie-overflow after eviction freezes the heap';
SELECT a FROM (SELECT if(number < 2000, number % 10, 0)::UInt32 AS a, number AS b FROM numbers(1300000)) GROUP BY a, b ORDER BY a ASC LIMIT 5
SETTINGS log_comment = '04338_tie_overflow', max_bytes_ratio_before_external_group_by = 0 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['AggregationTopKKeysEvicted']) > 0 AS evicted, sum(ProfileEvents['AggregationTopKHeapsFrozen']) > 0 AS froze
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04338_tie_overflow';

-- Every group the frozen-heap query returns must carry its complete count
-- (the freeze must not drop or corrupt accumulated state).
SELECT 'tie-overflow: every returned group complete';
SELECT count(), countIf(complete) FROM
(
    SELECT l.c = f.c AS complete
    FROM (SELECT a, b, count() AS c FROM (SELECT if(number < 2000, number % 10, 0)::UInt32 AS a, number AS b FROM numbers(1300000)) GROUP BY a, b ORDER BY a ASC LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) AS l
    INNER JOIN (SELECT a, b, count() AS c FROM (SELECT if(number < 2000, number % 10, 0)::UInt32 AS a, number AS b FROM numbers(1300000)) GROUP BY a, b SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (a, b)
);
