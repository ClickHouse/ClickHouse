-- Tags: no-fasttest, no-cpu-s390x
-- ^ DataSketches is not built in fast-test builds.

-- A sketch that another system wrote without a length (for example, Spark `hll_sketch_agg` into Parquet) is read as a state with
-- `addVarUIntLengthPrefix`, and the bytes of a state are given to another system with `removeVarUIntLengthPrefix`.

SELECT 'it can be read after the length is added';
SELECT finalizeAggregation(CAST(addVarUIntLengthPrefix(unhex('0201070b03080508cbd7c2042bf2fb06862ff90d7581660781bc5d06')), 'AggregateFunction(uniqApacheHLL, UInt64)'));

SELECT 'merged without a state column';
SELECT uniqApacheHLLMerge(CAST(addVarUIntLengthPrefix(sketch), 'AggregateFunction(uniqApacheHLL, UInt64)'))
FROM (SELECT unhex('0201070b03080508cbd7c2042bf2fb06862ff90d7581660781bc5d06') AS sketch FROM numbers(3));

SELECT 'a state gives the bytes of its sketch';
SELECT hex(substring(removeVarUIntLengthPrefix(toString(uniqApacheHLLState(11, 'HLL_8')(number))), 1, 8)) FROM numbers(10);

SELECT 'a sketch that is read and written back is the same sketch';
WITH unhex('0201070b03080508cbd7c2042bf2fb06862ff90d7581660781bc5d06') AS sketch
SELECT removeVarUIntLengthPrefix(toString(CAST(addVarUIntLengthPrefix(sketch), 'AggregateFunction(uniqApacheHLL, UInt64)'))) = sketch;

SELECT 'a big sketch needs a longer length';
WITH (SELECT removeVarUIntLengthPrefix(toString(uniqApacheHLLState(14, 'HLL_8')(number))) FROM numbers(100000)) AS sketch
SELECT length(sketch) > 16000, finalizeAggregation(CAST(addVarUIntLengthPrefix(sketch), 'AggregateFunction(uniqApacheHLL(14, \'HLL_8\'), UInt64)')) = uniqApacheHLL(14, 'HLL_8')(number) FROM numbers(100000);

SELECT 'a state that is not a length and bytes is rejected';
SELECT removeVarUIntLengthPrefix(toString(countState())); -- { serverError INCORRECT_DATA }
