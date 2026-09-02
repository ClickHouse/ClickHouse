DROP TABLE IF EXISTS grop_uniq_array_date;
CREATE TABLE grop_uniq_array_date (d Date, dt DateTime, id Integer) ENGINE = Memory;
INSERT INTO grop_uniq_array_date VALUES (toDate('2016-12-16'), toDateTime('2016-12-16 12:00:00'), 1) (toDate('2016-12-16'), toDateTime('2016-12-16 12:00:00'), 1);
SELECT groupUniqArray(d), groupUniqArray(dt) FROM grop_uniq_array_date;
INSERT INTO grop_uniq_array_date VALUES (toDate('2016-12-17'), toDateTime('2016-12-17 12:00:00'), 1), (toDate('2016-12-18'), toDateTime('2016-12-18 12:00:00'), 1), (toDate('2016-12-16'), toDateTime('2016-12-16 12:00:00'), 2);
SELECT length(groupUniqArray(2)(d)), length(groupUniqArray(2)(dt)), length(groupUniqArray(d)), length(groupUniqArray(dt)) FROM grop_uniq_array_date GROUP BY id ORDER BY id;
SELECT length(groupUniqArray(10000)(d)), length(groupUniqArray(10000)(dt)) FROM grop_uniq_array_date GROUP BY id ORDER BY id;
DROP TABLE IF EXISTS grop_uniq_array_date;
