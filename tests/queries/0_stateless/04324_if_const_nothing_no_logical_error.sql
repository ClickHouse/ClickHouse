SELECT if(arraySort(x -> x, [])[toNullable(1)], 1, 2);
SELECT multiIf(arraySort(x -> x, [])[toNullable(1)], 1, 2);
SELECT if(arrayMap(x -> x, [])[toNullable(1)], 1, 2);
SELECT if(arrayFilter(x -> x > 0, [])[toNullable(1)], 1, 2);
SELECT if(CAST(NULL, 'Nullable(Nothing)'), 1, 2);
SELECT multiIf(CAST(NULL, 'Nullable(Nothing)'), 1, 2);
