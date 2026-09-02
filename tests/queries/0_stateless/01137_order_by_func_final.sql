DROP TABLE IF EXISTS pk_func;
CREATE TABLE pk_func(d DateTime, ui UInt32) ENGINE = SummingMergeTree ORDER BY toDate(d);

INSERT INTO pk_func SELECT '2020-05-05 01:00:00', number FROM numbers(100000);
INSERT INTO pk_func SELECT '2020-05-06 01:00:00', number FROM numbers(100000);
INSERT INTO pk_func SELECT '2020-05-07 01:00:00', number FROM numbers(100000);

SELECT toDate(d), ui FROM pk_func FINAL order by d;

DROP TABLE pk_func;
