-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- mergeSerializedHLL without parameters must not degrade input sketches to the
-- default precision (lg_k=10) or rewrite their representation (HLL_4).
-- The serialized Apache DataSketches HLL preamble stores lg_k in byte 4
-- and the target type in bits 2-3 of byte 8 (0 = HLL_4, 1 = HLL_6, 2 = HLL_8).

SELECT 'no-parameter merge preserves lg_k and representation';
WITH
    (SELECT serializedHLL(14, 'HLL_8')(number) FROM numbers(100000)) AS sk,
    (SELECT mergeSerializedHLL(x) FROM (SELECT sk AS x)) AS merged
SELECT
    reinterpretAsUInt8(substring(merged, 4, 1)) AS lg_k,
    bitAnd(bitShiftRight(reinterpretAsUInt8(substring(merged, 8, 1)), 2), 3) AS target_type,
    cardinalityFromHLL(merged) = cardinalityFromHLL(sk) AS same_estimate;

SELECT 'no-parameter merge of two high-precision sketches keeps their precision';
WITH
    sketches AS
    (
        SELECT serializedHLL(14, 'HLL_8')(number) AS sk FROM numbers(50000)
        UNION ALL
        SELECT serializedHLL(14, 'HLL_8')(number + 25000) AS sk FROM numbers(50000)
    ),
    (SELECT mergeSerializedHLL(sk) FROM sketches) AS merged
SELECT
    reinterpretAsUInt8(substring(merged, 4, 1)) AS lg_k,
    bitAnd(bitShiftRight(reinterpretAsUInt8(substring(merged, 8, 1)), 2), 3) AS target_type,
    cardinalityFromHLL(merged) BETWEEN 73000 AND 77000 AS estimate_in_range;

SELECT 'explicit parameters still take precedence';
WITH
    (SELECT serializedHLL(14, 'HLL_8')(number) FROM numbers(100000)) AS sk,
    (SELECT mergeSerializedHLL(0, 10, 'HLL_4')(x) FROM (SELECT sk AS x)) AS merged
SELECT
    reinterpretAsUInt8(substring(merged, 4, 1)) AS lg_k,
    bitAnd(bitShiftRight(reinterpretAsUInt8(substring(merged, 8, 1)), 2), 3) AS target_type;

SELECT 'default sketches merged without parameters keep the default precision';
WITH
    (SELECT serializedHLL(number) FROM numbers(100000)) AS sk,
    (SELECT mergeSerializedHLL(x) FROM (SELECT sk AS x)) AS merged
SELECT
    reinterpretAsUInt8(substring(merged, 4, 1)) AS lg_k,
    bitAnd(bitShiftRight(reinterpretAsUInt8(substring(merged, 8, 1)), 2), 3) AS target_type,
    cardinalityFromHLL(merged) = cardinalityFromHLL(sk) AS same_estimate;
