SET max_threads = 1;
SET max_block_size = 2;
SET max_untracked_memory = 0;
SET optimize_distinct_in_order = 0;
SET allow_preliminary_distinct_abandoning = 0;
SET max_bytes_ratio_before_external_distinct = 0;

-- Hash-backed aggregate states can change their serialization when reconstructed. The repeated states
-- must match their original fingerprints after spilling, including inside composite and nested keys.
CREATE VIEW external_distinct_state_identity AS
SELECT
    number % 2 AS id,
    initializeAggregation('uniqExactArrayState',
        arrayMap(x -> cityHash64(x + id * 1000000), range(63))) AS s
FROM numbers(4);

SET max_bytes_before_external_distinct = 0;
SELECT 'spill_threshold=0';

SELECT 'single_state', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT s
    FROM external_distinct_state_identity
);

SELECT 'state_first', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT s, toString(id) AS suffix
    FROM external_distinct_state_identity
);

SELECT 'state_last', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT toString(id) AS prefix, s
    FROM external_distinct_state_identity
);

SELECT 'state_middle', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT if(id = 0, NULL, repeat('x', id)) AS prefix, s, range(id) AS suffix
    FROM external_distinct_state_identity
);

SELECT 'two_states', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT s, initializeAggregation('sumState', toUInt64(id)) AS other
    FROM external_distinct_state_identity
);

SELECT 'two_unstable_states', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT s, initializeAggregation('uniqExactArrayState', arrayMap(x -> cityHash64(x + id * 2000000 + 5000000), range(63))) AS other
    FROM external_distinct_state_identity
);

SELECT 'constants', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT 7 AS first_constant, s, 'constant' AS last_constant, id
    FROM external_distinct_state_identity
);

SELECT 'tuple_state', count(), arraySort(groupArray(finalizeAggregation(k.1))),
    uniqExactArrayMerge(k.1)
FROM
(
    SELECT DISTINCT tuple(s, id) AS k
    FROM external_distinct_state_identity
);

SELECT 'array_state', count(), arraySort(groupArray(finalizeAggregation(k[1]))),
    uniqExactArrayMerge(k[1])
FROM
(
    SELECT DISTINCT [s] AS k
    FROM external_distinct_state_identity
);

SELECT 'map_state', count(), arraySort(groupArray(finalizeAggregation(k['state']))),
    uniqExactArrayMerge(k['state'])
FROM
(
    SELECT DISTINCT map('state', s) AS k
    FROM external_distinct_state_identity
);

SELECT 'variant_state', count(), arraySort(groupArray(finalizeAggregation(variantElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))')))),
    uniqExactArrayMerge(variantElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))'))
FROM
(
    SELECT DISTINCT CAST(s, 'Variant(AggregateFunction(uniqExactArray, Array(UInt64)), String)') AS k
    FROM external_distinct_state_identity
);

SELECT 'dynamic_state', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))')))),
    uniqExactArrayMerge(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))'))
FROM
(
    SELECT DISTINCT CAST(s, 'Dynamic') AS k
    FROM external_distinct_state_identity
);

SELECT 'shared_dynamic_state', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))')))),
    uniqExactArrayMerge(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))'))
FROM
(
    SELECT DISTINCT CAST(s, 'Dynamic(max_types=0)') AS k
    FROM external_distinct_state_identity
);

SELECT 'variant_before_suffix', count(), arraySort(groupArray(finalizeAggregation(variantElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))')))),
    uniqExactArrayMerge(variantElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))'))
FROM
(
    SELECT DISTINCT CAST(s, 'Variant(AggregateFunction(uniqExactArray, Array(UInt64)), String)') AS k, toString(id) AS suffix
    FROM external_distinct_state_identity
);

SELECT 'shared_dynamic_before_suffix', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))')))),
    uniqExactArrayMerge(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))'))
FROM
(
    SELECT DISTINCT CAST(s, 'Dynamic(max_types=0)') AS k, toString(id) AS suffix
    FROM external_distinct_state_identity
);

-- Restoring input order preserves the state payload alongside its original key identity.
SELECT 'ordered_states', count(), groupArray(id), uniqExactArrayMerge(s)
FROM (SELECT DISTINCT s, id FROM external_distinct_state_identity ORDER BY id);


SET max_bytes_before_external_distinct = 1;
SELECT 'spill_threshold=1';

SELECT 'single_state', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT s
    FROM external_distinct_state_identity
);

SELECT 'state_first', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT s, toString(id) AS suffix
    FROM external_distinct_state_identity
);

SELECT 'state_last', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT toString(id) AS prefix, s
    FROM external_distinct_state_identity
);

SELECT 'state_middle', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT if(id = 0, NULL, repeat('x', id)) AS prefix, s, range(id) AS suffix
    FROM external_distinct_state_identity
);

SELECT 'two_states', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT s, initializeAggregation('sumState', toUInt64(id)) AS other
    FROM external_distinct_state_identity
);

SELECT 'two_unstable_states', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT s, initializeAggregation('uniqExactArrayState', arrayMap(x -> cityHash64(x + id * 2000000 + 5000000), range(63))) AS other
    FROM external_distinct_state_identity
);

SELECT 'constants', count(), arraySort(groupArray(finalizeAggregation(s))),
    uniqExactArrayMerge(s)
FROM
(
    SELECT DISTINCT 7 AS first_constant, s, 'constant' AS last_constant, id
    FROM external_distinct_state_identity
);

SELECT 'tuple_state', count(), arraySort(groupArray(finalizeAggregation(k.1))),
    uniqExactArrayMerge(k.1)
FROM
(
    SELECT DISTINCT tuple(s, id) AS k
    FROM external_distinct_state_identity
);

SELECT 'array_state', count(), arraySort(groupArray(finalizeAggregation(k[1]))),
    uniqExactArrayMerge(k[1])
FROM
(
    SELECT DISTINCT [s] AS k
    FROM external_distinct_state_identity
);

SELECT 'map_state', count(), arraySort(groupArray(finalizeAggregation(k['state']))),
    uniqExactArrayMerge(k['state'])
FROM
(
    SELECT DISTINCT map('state', s) AS k
    FROM external_distinct_state_identity
);

SELECT 'variant_state', count(), arraySort(groupArray(finalizeAggregation(variantElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))')))),
    uniqExactArrayMerge(variantElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))'))
FROM
(
    SELECT DISTINCT CAST(s, 'Variant(AggregateFunction(uniqExactArray, Array(UInt64)), String)') AS k
    FROM external_distinct_state_identity
);

SELECT 'dynamic_state', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))')))),
    uniqExactArrayMerge(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))'))
FROM
(
    SELECT DISTINCT CAST(s, 'Dynamic') AS k
    FROM external_distinct_state_identity
);

SELECT 'shared_dynamic_state', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))')))),
    uniqExactArrayMerge(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))'))
FROM
(
    SELECT DISTINCT CAST(s, 'Dynamic(max_types=0)') AS k
    FROM external_distinct_state_identity
);

SELECT 'variant_before_suffix', count(), arraySort(groupArray(finalizeAggregation(variantElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))')))),
    uniqExactArrayMerge(variantElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))'))
FROM
(
    SELECT DISTINCT CAST(s, 'Variant(AggregateFunction(uniqExactArray, Array(UInt64)), String)') AS k, toString(id) AS suffix
    FROM external_distinct_state_identity
);

SELECT 'shared_dynamic_before_suffix', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))')))),
    uniqExactArrayMerge(dynamicElement(k, 'AggregateFunction(uniqExactArray, Array(UInt64))'))
FROM
(
    SELECT DISTINCT CAST(s, 'Dynamic(max_types=0)') AS k, toString(id) AS suffix
    FROM external_distinct_state_identity
);

-- Restoring input order preserves the state payload alongside its original key identity.
SELECT 'ordered_states', count(), groupArray(id), uniqExactArrayMerge(s)
FROM (SELECT DISTINCT s, id FROM external_distinct_state_identity ORDER BY id);

DROP VIEW external_distinct_state_identity;
