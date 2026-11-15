-- Tags: no-fasttest, no-cpu-s390x
-- ^ DataSketches is not built in fast-test builds.

-- A sketch that is read as a state of another type, for example one with a lower `lg_k`, is kept as it was written.
-- Reading must not lose anything that writing the state back would then persist. The sketch is brought down to the
-- declared resolution only when it is merged.

SELECT 'a state read as a type with a lower lg_k and another storage type is written back unchanged';
WITH (SELECT hex(toString(uniqApacheHLLState(14, 'HLL_6')(number))) FROM numbers(100000)) AS h
SELECT hex(toString(CAST(unhex(h), 'AggregateFunction(uniqApacheHLL(8, \'HLL_4\'), UInt64)'))) = h;

SELECT 'the same for the default parameters';
WITH (SELECT hex(toString(uniqApacheHLLState(14, 'HLL_8')(number))) FROM numbers(100000)) AS h
SELECT hex(toString(CAST(unhex(h), 'AggregateFunction(uniqApacheHLL, UInt64)'))) = h;

SELECT 'the estimate of the imported state is the estimate of the sketch';
WITH (SELECT uniqApacheHLLState(14)(number) FROM numbers(100000)) AS s
SELECT finalizeAggregation(CAST(unhex(hex(toString(s))), 'AggregateFunction(uniqApacheHLL(8), UInt64)')) = finalizeAggregation(s);

SELECT 'merging brings it down to the declared resolution';
WITH (SELECT hex(toString(uniqApacheHLLState(14)(number))) FROM numbers(100000)) AS h
SELECT length(toString(uniqApacheHLLMergeState(8)(CAST(unhex(h), 'AggregateFunction(uniqApacheHLL(8), UInt64)')))) < length(unhex(h)) / 10;

SELECT 'an empty imported state merges to nothing';
SELECT uniqApacheHLLMerge(s) FROM
(
    SELECT CAST(unhex('080201070C030C0000'), 'AggregateFunction(uniqApacheHLL, UInt64)') AS s
    UNION ALL
    SELECT uniqApacheHLLState(number) AS s FROM numbers(3)
);
