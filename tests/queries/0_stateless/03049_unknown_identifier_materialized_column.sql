-- https://github.com/ClickHouse/ClickHouse/issues/54317
SET enable_analyzer=1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};
USE {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE l (y String) Engine Memory;
CREATE TABLE r (d Date, y String, ty UInt16 MATERIALIZED toYear(d)) Engine Memory;
select * from l L left join r R on  L.y = R.y  where R.ty >= 2019;
select * from l left join r  on  l.y = r.y  where r.ty >= 2019;
select * from {CLICKHOUSE_DATABASE:Identifier}.l left join {CLICKHOUSE_DATABASE:Identifier}.r  on  l.y = r.y  where r.ty >= 2019;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
