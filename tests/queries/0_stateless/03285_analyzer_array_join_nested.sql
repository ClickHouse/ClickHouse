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

select y, y.b.first, y.b.second from tab array join x as y;
