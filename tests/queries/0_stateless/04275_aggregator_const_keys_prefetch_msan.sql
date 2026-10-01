-- Regression test for STID 1553-34d7:
-- MemorySanitizer reported use-of-uninitialized-value at
-- `IColumnHelper<IColumnDummy, IColumn>::serializeValueIntoArenaWithNull`
-- in `arm_msan` serverfuzz. The MSan stack revealed that the uninitialized
-- byte was the `PODArray` padding of a 1-row `ColumnNullable<ColumnNothing>`
-- null map.
--
-- Root cause: `Aggregator::executeImplBatch` enters a fast path when all
-- GROUP BY keys are constants. The outer loop iterates over the key columns'
-- 1 row (`key_end = 1`), but the prefetch branch bounded the look-ahead by
-- `row_end` (the full block size). When `prefetch_look_ahead = 4`, the
-- prefetch called `state.getKeyHolder(4, pool)` against a 1-row key column,
-- which `HashMethodSerialized::getKeyHolder` then fed into
-- `serializeValueIntoArenaWithNull` reading `is_null[4]` past the valid byte.
-- The fix replaces `row_end` with `key_end` in that prefetch branch.

SET optimize_group_by_constant_keys = 1;
SET enable_software_prefetch_in_aggregation = 1;
SET allow_experimental_nullable_tuple_type = 1;

-- Pattern derived from the AST fuzzer query that produced the report:
-- all-constant tuple keys force the aggregator's `all_keys_are_const` path
-- while the variable-size tuple/`Nullable(Nothing)` shapes route through
-- `HashMethodSerialized`. The query intentionally has enough rows to make
-- the prefetch loop fire on the first iteration.
SELECT count() AS c
FROM numbers(10000)
GROUP BY
    CAST(tuple(NULL, NULL), 'Nullable(Tuple(Nullable(UInt32), Nullable(UInt32)))'),
    tuple(1, NULL),
    tuple(NULL);

SELECT count() AS c
FROM numbers(10000)
GROUP BY
    tuple(NULL, NULL),
    tuple(materialize(NULL), materialize(NULL)),
    'k';
