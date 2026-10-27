-- https://github.com/ClickHouse/ClickHouse/issues/58985

DROP TABLE IF EXISTS test_03093;

CREATE TABLE test_03093 (app String, c UInt64, k Map(String, String)) ENGINE=MergeTree ORDER BY app;

INSERT INTO test_03093 VALUES ('x1', 123, {'k1': ''});
INSERT INTO test_03093 VALUES ('x1', 123, {'k1': '', 'k11': ''});
INSERT INTO test_03093 VALUES ('x1', 12,  {'k1': ''});

SET enable_analyzer=1;

select app, arrayZip(untuple(sumMap(k.keys, replicate(1, k.keys)))) from test_03093 PREWHERE c > 1 group by app;
select app, arrayZip(untuple(sumMap(k.keys, replicate(1, k.keys)))) from test_03093 WHERE c > 1 group by app;

DROP TABLE IF EXISTS test_03093;
