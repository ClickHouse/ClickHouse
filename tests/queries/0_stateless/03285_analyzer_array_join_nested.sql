set enable_analyzer = 1;

create table hourly(
  hour datetime,
  `metric.names` Array(String),
  `metric.values` Array(Int64)
) Engine=Memory
as select '2020-01-01', ['a', 'b'], [1,2];

-- { echoOn }

explain query tree dump_ast = 1
SELECT
  `metric.names`
from hourly ARRAY JOIN metric;

explain query tree dump_ast = 1
SELECT
  metric.names
from hourly ARRAY JOIN metric;

-- { echoOff }

create table tab (`x.a` Array(String), `x.b.first` Array(Array(UInt32)), `x.b.second` Array(Array(String))) engine = MergeTree order by tuple();

insert into tab select ['a1', 'a2'], [[1, 2, 3], [4, 5, 6]], [['b1', 'b2', 'b3'], ['b4', 'b5', 'b6']];

-- { echoOn }

SELECT nested(['click', 'house'], x.b.first, x.b.second) AS n, toTypeName(n) FROM tab;
SELECT nested([['click', 'house']], x.b.first, x.b.second) AS n, toTypeName(n) FROM tab;
SELECT nested([['click'], ['house']], x.b.first, x.b.second) AS n, toTypeName(n) FROM tab; -- {serverError BAD_ARGUMENTS}

set analyzer_compatibility_allow_compound_identifiers_in_unflatten_nested = 0;
select x from tab;
select y, y.b.first, y.b.second from tab array join x as y; -- { serverError UNKNOWN_IDENTIFIER }

set analyzer_compatibility_allow_compound_identifiers_in_unflatten_nested = 1;
select x from tab;
select y, y.b.first, y.b.second from tab array join x as y;
