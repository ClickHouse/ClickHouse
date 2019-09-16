DROP TABLE IF EXISTS test_groupArray;

CREATE TABLE test_groupArray (`ids` Array(UInt32), `name` String) ENGINE = Log;
INSERT INTO test_groupArray SELECT groupArray(number),'test' FROM (SELECT number FROM system.numbers LIMIT 33554433); -- Because the default 128MB can store 33554432 UInt32 type data, so write 33554433 UInt32 type data here.

SELECT count(*) FROM (SELECT groupArray(ids) FROM test_groupArray WHERE name ='test' GROUP BY name);

DROP TABLE IF EXISTS test_groupArray;
