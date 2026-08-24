set enable_named_columns_in_function_tuple = 0;

select arrayMap(x -> 2 * x, []);
select toTypeName(arrayMap(x -> 2 * x, []));
select arrayMap((x, y) -> x + y, [], []);
select toTypeName(arrayMap((x, y) -> x + y, [], []));
select arrayMap((x, y) -> x + y, [], CAST([], 'Array(Int32)'));
select toTypeName(arrayMap((x, y) -> x + y, [], CAST([], 'Array(Int32)')));

select arrayFilter(x -> 2 * x < 0, []);
select toTypeName(arrayFilter(x -> 2 * x < 0, []));

select toTypeName(arrayMap(x -> CAST(x, 'String'), []));
select toTypeName(arrayMap(x -> toInt32(x), []));
select toColumnTypeName(arrayMap(x -> toInt32(x), []));

select toTypeName(arrayMap(x -> [x], []));
select toColumnTypeName(arrayMap(x -> [x], []));

select toTypeName(arrayMap(x ->map(1, x), []));
select toColumnTypeName(arrayMap(x -> map(1, x), []));

select toTypeName(arrayMap(x ->tuple(x), []));
select toColumnTypeName(arrayMap(x -> tuple(1, x), []));

select toTypeName(toInt32(assumeNotNull(materialize(NULL))));
select toColumnTypeName(toInt32(assumeNotNull(materialize(NULL))));

select toTypeName(assumeNotNull(materialize(NULL)));
select toColumnTypeName(assumeNotNull(materialize(NULL)));

select toTypeName([assumeNotNull(materialize(NULL))]);
select toColumnTypeName([assumeNotNull(materialize(NULL))]);

select toTypeName(map(1, assumeNotNull(materialize(NULL))));
select toColumnTypeName(map(1, assumeNotNull(materialize(NULL))));

select toTypeName(tuple(1, assumeNotNull(materialize(NULL))));
select toColumnTypeName(tuple(1, assumeNotNull(materialize(NULL))));

select toTypeName(assumeNotNull(materialize(NULL)) * 2);
select toColumnTypeName(assumeNotNull(materialize(NULL)) * 2);
