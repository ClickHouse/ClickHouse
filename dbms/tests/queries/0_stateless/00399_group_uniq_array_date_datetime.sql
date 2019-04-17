DROP TABLE IF EXISTS grop_uniq_array_date;
CREATE TABLE grop_uniq_array_date (d Date, dt DateTime) ENGINE = Memory;
INSERT INTO grop_uniq_array_date VALUES (toDate('2016-12-16'), toDateTime('2016-12-16 12:00:00')) (toDate('2016-12-16'), toDateTime('2016-12-16 12:00:00'));
SELECT groupUniqArray(d), groupUniqArray(dt) FROM grop_uniq_array_date;
DROP TABLE IF EXISTS grop_uniq_array_date;
