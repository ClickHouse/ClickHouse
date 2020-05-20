DROP TABLE IF EXISTS empty_pk;
CREATE TABLE empty_pk (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 256;

INSERT INTO empty_pk SELECT number FROM numbers(100000);

SELECT sum(x) from empty_pk;

DROP TABLE empty_pk;
