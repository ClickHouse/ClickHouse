DROP TABLE IF EXISTS buf_dest;
DROP TABLE IF EXISTS buf;

CREATE TABLE buf_dest (timestamp DateTime)
ENGINE = MergeTree PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp);

CREATE TABLE buf (timestamp DateTime) Engine = Buffer(currentDatabase(), buf_dest, 16, 86400, 86400, 2000000, 20000000, 100000000, 300000000);;

INSERT INTO buf (timestamp) VALUES (toDateTime('2020-01-01 00:05:00'));

OPTIMIZE TABLE buf;

ALTER TABLE buf_dest ADD COLUMN s String;
ALTER TABLE buf ADD COLUMN s String;

SELECT * FROM buf;

INSERT INTO buf (timestamp, s) VALUES (toDateTime('2020-01-01 00:06:00'), 'hello');

SELECT * FROM buf ORDER BY timestamp;

DROP TABLE IF EXISTS buf;
DROP TABLE IF EXISTS buf_dest;
