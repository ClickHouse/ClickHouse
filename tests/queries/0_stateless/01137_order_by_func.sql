DROP TABLE IF EXISTS pk_func;
CREATE TABLE pk_func(d DateTime, ui UInt32) ENGINE = MergeTree ORDER BY toDate(d);

INSERT INTO pk_func SELECT '2020-05-05 01:00:00', number FROM numbers(1000000);
INSERT INTO pk_func SELECT '2020-05-06 01:00:00', number FROM numbers(1000000);
INSERT INTO pk_func SELECT '2020-05-07 01:00:00', number FROM numbers(1000000);

SELECT * FROM pk_func ORDER BY toDate(d), ui LIMIT 5;

DROP TABLE pk_func;

CREATE TABLE pk_func(i UInt32) ENGINE = MergeTree ORDER BY -i;
INSERT INTO pk_func SELECT number FROM numbers(1000000);
INSERT INTO pk_func SELECT number FROM numbers(1000000);
INSERT INTO pk_func SELECT number FROM numbers(1000000);

SELECT * FROM pk_func ORDER BY -i LIMIT 5;

DROP TABLE pk_func;
