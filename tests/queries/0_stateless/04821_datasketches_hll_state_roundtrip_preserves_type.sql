-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- `mergeSerializedHLL` with omitted parameters must preserve the inferred lg_k and
-- representation (`HLL_6`/`HLL_8`) of its inputs across an aggregate-state round-trip
-- (`write`/`read` of the partial state, as happens under partial or distributed
-- aggregation). Storing the state in a `MergeTree` table and reading it back forces
-- the round-trip. The serialized Apache DataSketches HLL preamble stores lg_k in
-- byte 4 and the target type in bits 2-3 of byte 8 (0 = HLL_4, 1 = HLL_6, 2 = HLL_8).

DROP TABLE IF EXISTS hll_states_04821;
CREATE TABLE hll_states_04821 (st AggregateFunction(mergeSerializedHLL, String)) ENGINE = MergeTree ORDER BY tuple();

SELECT 'state round-trip preserves inferred lg_k and HLL_8 representation';
INSERT INTO hll_states_04821 SELECT mergeSerializedHLLState(sk) FROM (SELECT serializedHLL(14, 'HLL_8')(number) AS sk FROM numbers(100000));
WITH (SELECT mergeSerializedHLLMerge(st) FROM hll_states_04821) AS merged
SELECT
    reinterpretAsUInt8(substring(merged, 4, 1)) AS lg_k,
    bitAnd(bitShiftRight(reinterpretAsUInt8(substring(merged, 8, 1)), 2), 3) AS target_type,
    cardinalityFromHLL(merged) BETWEEN 95000 AND 105000 AS estimate_in_range;

SELECT 'state round-trip preserves inferred HLL_6 representation';
TRUNCATE TABLE hll_states_04821;
INSERT INTO hll_states_04821 SELECT mergeSerializedHLLState(sk) FROM (SELECT serializedHLL(14, 'HLL_6')(number) AS sk FROM numbers(100000));
WITH (SELECT mergeSerializedHLLMerge(st) FROM hll_states_04821) AS merged
SELECT
    reinterpretAsUInt8(substring(merged, 4, 1)) AS lg_k,
    bitAnd(bitShiftRight(reinterpretAsUInt8(substring(merged, 8, 1)), 2), 3) AS target_type;

SELECT 'merging empty partial states keeps the empty-string result';
TRUNCATE TABLE hll_states_04821;
INSERT INTO hll_states_04821 SELECT mergeSerializedHLLState(x) FROM (SELECT '' AS x FROM numbers(10));
INSERT INTO hll_states_04821 SELECT mergeSerializedHLLState(x) FROM (SELECT '' AS x FROM numbers(10));
SELECT mergeSerializedHLLMerge(st) = '' FROM hll_states_04821;

SELECT 'single-stage aggregation over empty sketch strings agrees';
SELECT mergeSerializedHLL(x) = '' FROM (SELECT '' AS x FROM numbers(10));

DROP TABLE hll_states_04821;
