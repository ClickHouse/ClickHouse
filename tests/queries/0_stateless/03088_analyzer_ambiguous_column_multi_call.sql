-- https://github.com/ClickHouse/ClickHouse/issues/61014
SET enable_analyzer=1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
create database {CLICKHOUSE_DATABASE:Identifier};

create table {CLICKHOUSE_DATABASE:Identifier}.a (i int) engine = Log();

select
  {CLICKHOUSE_DATABASE:Identifier}.a.i
from
  {CLICKHOUSE_DATABASE:Identifier}.a,
  {CLICKHOUSE_DATABASE:Identifier}.a as x;
