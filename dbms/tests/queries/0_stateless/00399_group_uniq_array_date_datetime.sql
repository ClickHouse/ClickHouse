DROP TABLE IF EXISTS test.grop_uniq_array_date;
CREATE TABLE test.grop_uniq_array_date (d Date, dt DateTime) ENGINE = Memory;
INSERT INTO test.grop_uniq_array_date VALUES (toDate('2016-12-16'), toDateTime('2016-12-16 12:00:00')) (toDate('2016-12-16'), toDateTime('2016-12-16 12:00:00'));
SELECT groupUniqArray(d), groupUniqArray(dt) FROM test.grop_uniq_array_date;
DROP TABLE IF EXISTS test.grop_uniq_array_date;
