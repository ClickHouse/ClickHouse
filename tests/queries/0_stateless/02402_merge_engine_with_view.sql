-- #40014
CREATE TABLE m0 (id UInt64) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity = 1, ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO m0 SELECT number FROM numbers(10);
CREATE TABLE m1 (id UInt64, s String) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity = 1, ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO m1 SELECT number, 'boo' FROM numbers(10);
CREATE VIEW m1v AS SELECT id FROM m1;

CREATE TABLE m2 (id UInt64) ENGINE=Merge(currentDatabase(),'m0|m1v');

SELECT * FROM m2 WHERE id > 1 AND id < 5 ORDER BY id SETTINGS force_primary_key=1, max_bytes_to_read=64;

-- #40706
CREATE VIEW v AS SELECT 1;
SELECT 1 FROM merge(currentDatabase(), '^v$');