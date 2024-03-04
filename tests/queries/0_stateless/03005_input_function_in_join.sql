create table test (a Int8) engine = MergeTree order by tuple();
INSERT INTO test
SELECT * FROM (
 SELECT number
 FROM system.numbers
 LIMIT 10
) AS x
INNER JOIN input('a UInt64') AS y ON x.number = y.a
Format CSV 42; -- {serverError INVALID_USAGE_OF_INPUT}
drop table test;

