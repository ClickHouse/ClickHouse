-- Tags: no-fasttest, no-cpu-s390x
-- ^ DataSketches is not built in fast-test builds.

-- The HIP estimate of the library depends on the order of the inserts and of the merges. The third parameter, `estimator`, chooses
-- the estimate: `DEFAULT` is the one of the library, `COMPOSITE` is computed from the registers of the sketch only, so it does not
-- depend on the order. Only the second is checked here, because the first one differs between orders only sometimes.

SET max_threads = 1;

-- 8 sketches of 100 values each, neighbours overlap by half, 450 distinct values in total, merged in three orders

SELECT 'the composite estimate of a merged state does not depend on the order of the merges';
SELECT count() = 3 AND uniqExact(r) = 1 AND any(r) BETWEEN 430 AND 470 FROM
(
    SELECT uniqApacheHLLMerge(11, 'HLL_8', 'COMPOSITE')(s) AS r FROM (SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(0) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(1) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(2) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(3) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(4) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(5) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(6) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(7) * 50 + number) AS s FROM numbers(100)) UNION ALL
    SELECT uniqApacheHLLMerge(11, 'HLL_8', 'COMPOSITE')(s) AS r FROM (SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(7) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(6) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(5) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(4) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(3) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(2) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(1) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(0) * 50 + number) AS s FROM numbers(100)) UNION ALL
    SELECT uniqApacheHLLMerge(11, 'HLL_8', 'COMPOSITE')(s) AS r FROM (SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(3) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(6) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(0) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(5) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(1) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(7) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(2) * 50 + number) AS s FROM numbers(100) UNION ALL SELECT uniqApacheHLLState(11, 'HLL_8')(toUInt64(4) * 50 + number) AS s FROM numbers(100))
);

SELECT 'the estimator does not change the state, and states of any estimator have one type where a common type is needed';
SELECT hex(toString(uniqApacheHLLState(11, 'HLL_8', 'COMPOSITE')(number))) = hex(toString(uniqApacheHLLState(11, 'HLL_8', 'DEFAULT')(number))) FROM numbers(1000);
SELECT length([uniqApacheHLLState(8, 'HLL_6', 'COMPOSITE')(toUInt64(1)), uniqApacheHLLState(toUInt64(2))]);

SELECT 'a small sketch has the same estimate';
SELECT uniqApacheHLL(11, 'HLL_8', 'COMPOSITE')(number), uniqApacheHLL(11, 'HLL_8', 'DEFAULT')(number) FROM numbers(100);

SELECT 'the composite estimate of a sketch that is built by inserts does not depend on the order of the inserts';
SELECT count() = 3 AND uniqExact(r) = 1 AND any(r) BETWEEN 2800 AND 3200 FROM
(
    SELECT uniqApacheHLL(11, 'HLL_8', 'COMPOSITE')(x) AS r FROM (SELECT number AS x FROM numbers(3000) ORDER BY x) UNION ALL
    SELECT uniqApacheHLL(11, 'HLL_8', 'COMPOSITE')(x) AS r FROM (SELECT number AS x FROM numbers(3000) ORDER BY -x) UNION ALL
    SELECT uniqApacheHLL(11, 'HLL_8', 'COMPOSITE')(x) AS r FROM (SELECT number AS x FROM numbers(3000) ORDER BY cityHash64(x))
);

SELECT 'the same for a state that is finalized, and for a state that is read';
SELECT count() = 3 AND uniqExact(r) = 1 FROM
(
    SELECT finalizeAggregation(uniqApacheHLLState(11, 'HLL_8', 'COMPOSITE')(x)) AS r FROM (SELECT number AS x FROM numbers(3000) ORDER BY x) UNION ALL
    SELECT finalizeAggregation(uniqApacheHLLState(11, 'HLL_8', 'COMPOSITE')(x)) AS r FROM (SELECT number AS x FROM numbers(3000) ORDER BY -x) UNION ALL
    SELECT finalizeAggregation(uniqApacheHLLState(11, 'HLL_8', 'COMPOSITE')(x)) AS r FROM (SELECT number AS x FROM numbers(3000) ORDER BY cityHash64(x))
);
SELECT count() = 3 AND uniqExact(r) = 1 FROM
(
    SELECT finalizeAggregation(CAST(toString(uniqApacheHLLState(11, 'HLL_8')(x)), 'AggregateFunction(uniqApacheHLL(11, \'HLL_8\', \'COMPOSITE\'), UInt64)')) AS r FROM (SELECT number AS x FROM numbers(3000) ORDER BY x) UNION ALL
    SELECT finalizeAggregation(CAST(toString(uniqApacheHLLState(11, 'HLL_8')(x)), 'AggregateFunction(uniqApacheHLL(11, \'HLL_8\', \'COMPOSITE\'), UInt64)')) AS r FROM (SELECT number AS x FROM numbers(3000) ORDER BY -x) UNION ALL
    SELECT finalizeAggregation(CAST(toString(uniqApacheHLLState(11, 'HLL_8')(x)), 'AggregateFunction(uniqApacheHLL(11, \'HLL_8\', \'COMPOSITE\'), UInt64)')) AS r FROM (SELECT number AS x FROM numbers(3000) ORDER BY cityHash64(x))
);

SELECT 'wrong parameters';
SELECT uniqApacheHLL(11, 'HLL_8', 'HIP')(number) FROM numbers(1); -- { serverError BAD_ARGUMENTS }
SELECT uniqApacheHLL(11, 'HLL_8', 1)(number) FROM numbers(1); -- { serverError BAD_ARGUMENTS }
SELECT uniqApacheHLL(11, 'HLL_8', 'COMPOSITE', 1)(number) FROM numbers(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
