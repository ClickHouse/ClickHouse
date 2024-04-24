-- https://github.com/ClickHouse/ClickHouse/issues/62890
SELECT sum(if(materialize(0), toNullable(1), 0));
SELECT sum(if(dummy, 0, toNullable(0)));

SELECT sum(if(s == '', v, 0)) b from VALUES ('v Nullable(Int64), s String',(1, 'x'));
