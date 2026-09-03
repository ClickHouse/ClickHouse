-- Coverage test for AggregateFunctionDistinctDynamicTypes.
-- Targets uncovered paths in src/AggregateFunctions/AggregateFunctionDistinctDynamicTypes.cpp:
--   lines 34-36:  AggregateFunctionDistinctDynamicTypesData::add
--   lines 38-41:  AggregateFunctionDistinctDynamicTypesData::merge
--   lines 67-76:  AggregateFunctionDistinctDynamicTypesData::insertResultInto
--   lines 84-98:  AggregateFunctionDistinctDynamicTypes constructor, getName, allocatesMemoryInArena, add
--   lines 123-126: mergeImpl
--   lines 138-141: insertResultInto
--   lines 146-156: factory function error paths (wrong argument count, non-Dynamic type)
-- Tags: no-random-settings

-- All-NULL input: result is empty array (add() skips NULLs via isNullAt check, line 95-96).
SELECT distinctDynamicTypes(d) FROM VALUES('d Dynamic', (NULL), (NULL));

-- Optimized path (addBatchSinglePlace, all rows, no filter): multiple distinct types, sorted output.
SELECT distinctDynamicTypes(d) FROM VALUES('d Dynamic', ('hello'), (42), (3.14), (NULL));

-- Row-by-row path: the If-variant sets if_argument_pos >= 0, which bypasses the
-- addBatchSinglePlace optimisation and calls add() (lines 92-98) once per row.
SELECT distinctDynamicTypesIf(d, cond) FROM VALUES('d Dynamic, cond UInt8', ('hello', 1), (42, 0), (3.14, 1));

-- Merge combinator: distinctDynamicTypesMerge forces mergeImpl (lines 123-126) and
-- AggregateFunctionDistinctDynamicTypesData::merge (lines 38-41) across two partial states.
SELECT distinctDynamicTypesMerge(s)
FROM (
    SELECT distinctDynamicTypesState(d) AS s
    FROM VALUES('d Dynamic', ('hello'), (42))
    UNION ALL
    SELECT distinctDynamicTypesState(d) AS s
    FROM VALUES('d Dynamic', (3.14))
);

-- GROUP BY also exercises add() per-row across multiple groups.
SELECT key, distinctDynamicTypes(d)
FROM VALUES('key UInt8, d Dynamic', (0, 'hello'), (0, 42), (1, 3.14), (1, NULL))
GROUP BY key
ORDER BY key;

-- Error: wrong number of arguments (lines 148-150 of factory).
SELECT distinctDynamicTypes(1, 2) FROM (SELECT 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Error: non-Dynamic argument type (lines 152-153 of factory).
SELECT distinctDynamicTypes(1) FROM (SELECT 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
