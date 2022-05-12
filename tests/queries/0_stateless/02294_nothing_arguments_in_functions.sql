select arrayMap(x -> 2 * x, []);
select toTypeName(arrayMap(x -> 2 * x, []));
select arrayMap((x, y) -> x + y, [], []);
select toTypeName(arrayMap((x, y) -> x + y, [], []));
select arrayMap((x, y) -> x + y, [], CAST([], 'Array(Int32)'));
select toTypeName(arrayMap((x, y) -> x + y, [], CAST([], 'Array(Int32)')));

select toTypeName(arrayMap(x -> 2 * x, [assumeNotNull(NULL)]));
select toColumnTypeName(arrayMap(x -> 2 * x, [assumeNotNull(NULL)]));

select arrayFilter(x -> 2 * x < 0, []);
select toTypeName(arrayFilter(x -> 2 * x < 0, []));
select toTypeName(arrayFilter(x -> 2 * x < 0, [assumeNotNull(NULL)]));
select toColumnTypeName(arrayFilter(x -> 2 * x < 0, [assumeNotNull(NULL)]));

select CAST(assumeNotNull(NULL), 'String');
select toTypeName(toInt32(assumeNotNull(NULL)));
select toColumnTypeName(toInt32(assumeNotNull(NULL)));

select toTypeName(assumeNotNull(NULL));
select toColumnTypeName(assumeNotNull(NULL));
select toTypeName(assumeNotNull(materialize(NULL)));
select toColumnTypeName(assumeNotNull(materialize(NULL)));

select toTypeName([assumeNotNull(NULL)]);
select toColumnTypeName([assumeNotNull(NULL)]);
select toTypeName([assumeNotNull(materialize(NULL))]);
select toColumnTypeName([assumeNotNull(materialize(NULL))]);

select toTypeName(map(1, assumeNotNull(NULL)));
select toColumnTypeName(map(1, assumeNotNull(NULL)));
select toTypeName(map(1, assumeNotNull(materialize(NULL))));
select toColumnTypeName(map(1, assumeNotNull(materialize(NULL))));

select toTypeName(tuple(1, assumeNotNull(NULL)));
select toColumnTypeName(tuple(1, assumeNotNull(NULL)));
select toTypeName(tuple(1, assumeNotNull(materialize(NULL))));
select toColumnTypeName(tuple(1, assumeNotNull(materialize(NULL))));

select toTypeName(assumeNotNull(NULL) * 2);
select toColumnTypeName(assumeNotNull(NULL) * 2);
select toTypeName(assumeNotNull(materialize(NULL)) * 2);
select toColumnTypeName(assumeNotNull(materialize(NULL)) * 2);
