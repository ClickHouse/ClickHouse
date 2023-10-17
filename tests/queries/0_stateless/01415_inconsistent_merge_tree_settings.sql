-- Tags: no-parallel

DROP TABLE IF EXISTS t;

SET mutations_sync = 1;
CREATE TABLE t (x UInt8, s String) ENGINE = MergeTree ORDER BY x SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 15;

INSERT INTO t VALUES (1, 'hello');
SELECT * FROM t;

ALTER TABLE t UPDATE s = 'world' WHERE x = 1;
SELECT * FROM t;

DROP TABLE t;
