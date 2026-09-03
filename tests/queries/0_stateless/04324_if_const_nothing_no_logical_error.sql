SELECT if(arraySort(x -> x, [])[toNullable(1)], 1, 2);
SELECT multiIf(arraySort(x -> x, [])[toNullable(1)], 1, 2);
SELECT if(arrayMap(x -> x, [])[toNullable(1)], 1, 2);
SELECT if(arrayFilter(x -> x > 0, [])[toNullable(1)], 1, 2);
SELECT if(CAST(NULL, 'Nullable(Nothing)'), 1, 2);
SELECT multiIf(CAST(NULL, 'Nullable(Nothing)'), 1, 2);

-- Short-circuit evaluation must also accept `Const(Nullable(Nothing))` conditions:
-- the lazy execution helpers call `getBool(0)` on the condition, which previously
-- aborted on `ColumnNullable(ColumnNothing)`. `throwIf(1)` proves the then-branch is
-- skipped (no exception) instead of evaluated.
SELECT if(arraySort(x -> x, [])[toNullable(1)], throwIf(1), 2) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT multiIf(arraySort(x -> x, [])[toNullable(1)], throwIf(1), 2) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT if(CAST(NULL, 'Nullable(Nothing)'), throwIf(1), 2) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT multiIf(CAST(NULL, 'Nullable(Nothing)'), throwIf(1), 2) SETTINGS short_circuit_function_evaluation = 'enable';
