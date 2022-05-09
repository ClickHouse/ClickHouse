select arrayMap(x -> 2 * x, []);
select toTypeName(arrayMap(x -> 2 * x, []));
select arrayMap((x, y) -> x + y, [], []);
select toTypeName(arrayMap((x, y) -> x + y, [], []));
select arrayMap((x, y) -> x + y, [], CAST([], 'Array(Int32)'));
select toTypeName(arrayMap((x, y) -> x + y, [], CAST([], 'Array(Int32)')));
