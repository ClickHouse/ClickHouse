SET max_untracked_memory = 0;
SET max_bytes_ratio_before_external_distinct = 0;
SET max_threads = 1;
SET max_block_size = 2;
SET optimize_distinct_in_order = 0;
SET allow_preliminary_distinct_abandoning = 0;

-- Small chunks exercise repeated keys across input and spill boundaries.
CREATE VIEW external_distinct_nested_keys AS
SELECT
    number,
    initializeAggregation('sumState', toUInt64(number % 3)) AS state,
    CAST(state, 'Variant(AggregateFunction(sum, UInt64), String)') AS variant,
    CAST(state, 'Dynamic') AS dynamic,
    CAST(state, 'Dynamic(max_types=0)') AS shared_dynamic
FROM numbers(20);

SET max_bytes_before_external_distinct = 0;
SELECT 'spill_threshold=0';

SELECT 'state', count(), arraySort(groupArray(finalizeAggregation(k)))
FROM (SELECT DISTINCT state AS k FROM external_distinct_nested_keys);

SELECT 'variant', count(), arraySort(groupArray(finalizeAggregation(variantElement(k, 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT variant AS k FROM external_distinct_nested_keys);

SELECT 'array_variant', count(), arraySort(groupArray(finalizeAggregation(variantElement(k[1], 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT [variant] AS k FROM external_distinct_nested_keys);

SELECT 'tuple_variant', count(), arraySort(groupArray(finalizeAggregation(variantElement(k.1, 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT tuple(variant) AS k FROM external_distinct_nested_keys);

SELECT 'map_variant', count(), arraySort(groupArray(finalizeAggregation(variantElement(k['state'], 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT map('state', variant) AS k FROM external_distinct_nested_keys);

SELECT 'variant_tuple', count(), arraySort(groupArray(finalizeAggregation(variantElement(k, 'Tuple(AggregateFunction(sum, UInt64))').1)))
FROM (SELECT DISTINCT CAST(tuple(state), 'Variant(Tuple(AggregateFunction(sum, UInt64)), String)') AS k FROM external_distinct_nested_keys);

SELECT 'dynamic', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k, 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT dynamic AS k FROM external_distinct_nested_keys);

SELECT 'shared_dynamic', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k, 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT shared_dynamic AS k FROM external_distinct_nested_keys);

SELECT 'array_dynamic', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k[1], 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT [dynamic] AS k FROM external_distinct_nested_keys);

SELECT 'json', count(), arraySort(groupArray(toUInt64(k.value)))
FROM (SELECT DISTINCT CAST(map('value', toUInt64(number % 3)), 'JSON') AS k FROM external_distinct_nested_keys);

SELECT 'shared_json', count(), arraySort(groupArray(toUInt64(k.value)))
FROM (SELECT DISTINCT CAST(map('value', toUInt64(number % 3)), 'JSON(max_dynamic_paths=0)') AS k FROM external_distinct_nested_keys);

SELECT 'typed_json', count(), arraySort(groupArray(k.value))
FROM (SELECT DISTINCT CAST(map('value', toUInt64(number % 3)), 'JSON(value UInt64)') AS k FROM external_distinct_nested_keys);

-- The first chunks contain strings; aggregate states and nulls arrive in subsequent chunks.
SELECT 'late_dynamic', count(), countIf(isNull(k)),
    arraySort(groupArrayIf(finalizeAggregation(dynamicElement(k, 'AggregateFunction(sum, UInt64)')), dynamicType(k) = 'AggregateFunction(sum, UInt64)')),
    arraySort(groupArrayIf(dynamicElement(k, 'String'), dynamicType(k) = 'String'))
FROM
(
    SELECT DISTINCT if(number < 4, CAST(toString(number), 'Dynamic'), if(number % 5 = 0, NULL, dynamic)) AS k
    FROM external_distinct_nested_keys
);

-- Comparable alternatives retain distinct type identities even when their displayed values coincide.
SELECT 'comparable_variant', count(), arraySort(groupArray(tuple(variantType(k), toString(k))))
FROM
(
    SELECT DISTINCT CAST(if(number % 2 = 0, toUInt64(number % 3), toString(number % 3)), 'Variant(UInt64, String)') AS k
    FROM external_distinct_nested_keys
)
SETTINGS use_variant_as_common_type = 1;

-- Variable-length aggregate states retain their contents when the spill keys are restored.
SELECT 'variable_length_state', count(),
    arraySort(groupArray(finalizeAggregation(variantElement(k, 'AggregateFunction(groupArray, String)'))[1]))
FROM
(
    SELECT DISTINCT CAST(initializeAggregation('groupArrayState', repeat('x', number % 3)),
        'Variant(AggregateFunction(groupArray, String), UInt64)') AS k
    FROM external_distinct_nested_keys
);

SET max_bytes_before_external_distinct = 1;
SELECT 'spill_threshold=1';

SELECT 'state', count(), arraySort(groupArray(finalizeAggregation(k)))
FROM (SELECT DISTINCT state AS k FROM external_distinct_nested_keys);

SELECT 'variant', count(), arraySort(groupArray(finalizeAggregation(variantElement(k, 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT variant AS k FROM external_distinct_nested_keys);

SELECT 'array_variant', count(), arraySort(groupArray(finalizeAggregation(variantElement(k[1], 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT [variant] AS k FROM external_distinct_nested_keys);

SELECT 'tuple_variant', count(), arraySort(groupArray(finalizeAggregation(variantElement(k.1, 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT tuple(variant) AS k FROM external_distinct_nested_keys);

SELECT 'map_variant', count(), arraySort(groupArray(finalizeAggregation(variantElement(k['state'], 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT map('state', variant) AS k FROM external_distinct_nested_keys);

SELECT 'variant_tuple', count(), arraySort(groupArray(finalizeAggregation(variantElement(k, 'Tuple(AggregateFunction(sum, UInt64))').1)))
FROM (SELECT DISTINCT CAST(tuple(state), 'Variant(Tuple(AggregateFunction(sum, UInt64)), String)') AS k FROM external_distinct_nested_keys);

SELECT 'dynamic', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k, 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT dynamic AS k FROM external_distinct_nested_keys);

SELECT 'shared_dynamic', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k, 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT shared_dynamic AS k FROM external_distinct_nested_keys);

SELECT 'array_dynamic', count(), arraySort(groupArray(finalizeAggregation(dynamicElement(k[1], 'AggregateFunction(sum, UInt64)'))))
FROM (SELECT DISTINCT [dynamic] AS k FROM external_distinct_nested_keys);

SELECT 'json', count(), arraySort(groupArray(toUInt64(k.value)))
FROM (SELECT DISTINCT CAST(map('value', toUInt64(number % 3)), 'JSON') AS k FROM external_distinct_nested_keys);

SELECT 'shared_json', count(), arraySort(groupArray(toUInt64(k.value)))
FROM (SELECT DISTINCT CAST(map('value', toUInt64(number % 3)), 'JSON(max_dynamic_paths=0)') AS k FROM external_distinct_nested_keys);

SELECT 'typed_json', count(), arraySort(groupArray(k.value))
FROM (SELECT DISTINCT CAST(map('value', toUInt64(number % 3)), 'JSON(value UInt64)') AS k FROM external_distinct_nested_keys);

-- The first chunks contain strings; aggregate states and nulls arrive in subsequent chunks.
SELECT 'late_dynamic', count(), countIf(isNull(k)),
    arraySort(groupArrayIf(finalizeAggregation(dynamicElement(k, 'AggregateFunction(sum, UInt64)')), dynamicType(k) = 'AggregateFunction(sum, UInt64)')),
    arraySort(groupArrayIf(dynamicElement(k, 'String'), dynamicType(k) = 'String'))
FROM
(
    SELECT DISTINCT if(number < 4, CAST(toString(number), 'Dynamic'), if(number % 5 = 0, NULL, dynamic)) AS k
    FROM external_distinct_nested_keys
);

-- Comparable alternatives retain distinct type identities even when their displayed values coincide.
SELECT 'comparable_variant', count(), arraySort(groupArray(tuple(variantType(k), toString(k))))
FROM
(
    SELECT DISTINCT CAST(if(number % 2 = 0, toUInt64(number % 3), toString(number % 3)), 'Variant(UInt64, String)') AS k
    FROM external_distinct_nested_keys
)
SETTINGS use_variant_as_common_type = 1;

-- Variable-length aggregate states retain their contents when the spill keys are restored.
SELECT 'variable_length_state', count(),
    arraySort(groupArray(finalizeAggregation(variantElement(k, 'AggregateFunction(groupArray, String)'))[1]))
FROM
(
    SELECT DISTINCT CAST(initializeAggregation('groupArrayState', repeat('x', number % 3)),
        'Variant(AggregateFunction(groupArray, String), UInt64)') AS k
    FROM external_distinct_nested_keys
);

DROP VIEW external_distinct_nested_keys;
