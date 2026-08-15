-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- With no explicit target type, `mergeSerializedHLL` chooses the highest input
-- representation. This makes the serialized result independent of input order.
WITH
    (SELECT serializedHLL(10, 'HLL_4')(number) FROM numbers(1000)) AS hll_4,
    (SELECT serializedHLL(10, 'HLL_8')(number) FROM numbers(1000)) AS hll_8
SELECT mergeSerializedHLL(sketch) =
    (SELECT mergeSerializedHLL(sketch) FROM (SELECT arrayJoin([hll_8, hll_4]) AS sketch))
FROM (SELECT arrayJoin([hll_4, hll_8]) AS sketch);
