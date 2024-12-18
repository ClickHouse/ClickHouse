-- https://github.com/ClickHouse/ClickHouse/issues/28687
SET enable_analyzer=1;
create view alias (dummy int, n alias dummy) as select * from system.one;

select n from alias;

select * from alias where n=0;
