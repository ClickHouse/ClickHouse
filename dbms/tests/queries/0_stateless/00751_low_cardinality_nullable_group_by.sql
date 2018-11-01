SET allow_experimental_low_cardinality_type = 1;
drop table if exists test.low_null_float;
CREATE TABLE test.low_null_float (a LowCardinality(Nullable(Float64))) ENGINE = MergeTree order by tuple();
INSERT INTO test.low_null_float (a) SELECT if(number % 3 == 0, Null, number)  FROM system.numbers LIMIT 1000000;

SELECT a, count() FROM test.low_null_float GROUP BY a ORDER BY count() desc, a LIMIT 10;
drop table if exists test.low_null_float;

