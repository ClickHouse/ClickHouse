drop table if exists low_null_float;
CREATE TABLE low_null_float (a LowCardinality(Nullable(Float64))) ENGINE = MergeTree order by tuple();
INSERT INTO low_null_float (a) SELECT if(number % 3 == 0, Null, number)  FROM system.numbers LIMIT 1000000;

SELECT a, count() FROM low_null_float GROUP BY a ORDER BY count() desc, a LIMIT 10;
drop table if exists low_null_float;

