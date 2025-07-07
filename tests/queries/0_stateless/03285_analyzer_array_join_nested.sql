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
