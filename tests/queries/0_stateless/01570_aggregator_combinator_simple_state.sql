with anySimpleState(number) as c select toTypeName(c), c from numbers(1);
with anyLastSimpleState(number) as c select toTypeName(c), c from numbers(1);
with minSimpleState(number) as c select toTypeName(c), c from numbers(1);
with maxSimpleState(number) as c select toTypeName(c), c from numbers(1);
with sumSimpleState(number) as c select toTypeName(c), c from numbers(1);
with sumWithOverflowSimpleState(number) as c select toTypeName(c), c from numbers(1);
with groupBitAndSimpleState(number) as c select toTypeName(c), c from numbers(1);
with groupBitOrSimpleState(number) as c select toTypeName(c), c from numbers(1);
with groupBitXorSimpleState(number) as c select toTypeName(c), c from numbers(1);
with sumMapSimpleState(([number], [number])) as c select toTypeName(c), c from numbers(1);
with minMapSimpleState(([number], [number])) as c select toTypeName(c), c from numbers(1);
with maxMapSimpleState(([number], [number])) as c select toTypeName(c), c from numbers(1);
with groupArrayArraySimpleState([number]) as c select toTypeName(c), c from numbers(1);
with groupUniqArrayArraySimpleState([number]) as c select toTypeName(c), c from numbers(1);

-- non-SimpleAggregateFunction
with countSimpleState(number) as c select toTypeName(c), c from numbers(1); -- { serverError 36 }
