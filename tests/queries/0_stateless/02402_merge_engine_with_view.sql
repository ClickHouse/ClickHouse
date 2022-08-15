
CREATE TABLE m0 (id UInt64) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO m0 SELECT number FROM numbers(10);
CREATE TABLE m1 (id UInt64, s String) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO m1 SELECT number, 'boo' FROM numbers(10);
CREATE VIEW m1v AS SELECT id FROM m1;

CREATE TABLE m2 (id UInt64) ENGINE=Merge(currentDatabase(),'m0|m1v');

SELECT * FROM m2 WHERE id > 1 AND id < 5 SETTINGS force_primary_key=1, max_bytes_to_read=64;