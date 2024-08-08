-- Tags: no-replicated-database

DROP TABLE IF EXISTS index_test;

CREATE TABLE index_test
(
	x UInt32,
	y UInt32,
	z UInt32,
    a UInt32,
    b UInt32,
    c UInt32
) ENGINE = MergeTree order by x
SETTINGS index_granularity=8192;

ALTER TABLE index_test
    ADD INDEX i_x mortonDecode(2, z).1 TYPE minmax GRANULARITY 1,
    ADD INDEX i_y mortonDecode(2, z).2 TYPE minmax GRANULARITY 1,
    MATERIALIZE INDEX i_x,
    MATERIALIZE INDEX i_y;

DROP TABLE index_test;

CREATE TABLE index_test
(
    a UInt64,
    b UInt64,
    c UInt64
) ENGINE MergeTree primary key a
SETTINGS index_granularity=8192;

INSERT INTO index_test (a, b, c) SELECT intDiv(number,4096), intDiv(number,4096), intDiv(number,4096) FROM numbers(1000000);

ALTER TABLE index_test ADD INDEX skip_i_a a TYPE set(100) GRANULARITY 2;
ALTER TABLE index_test ADD INDEX skip_i_b b TYPE set(100) GRANULARITY 2;

ALTER TABLE index_test MATERIALIZE INDEXES settings mutations_sync=1;

SELECT COUNT(b) FROM index_test WHERE b IN (125, 700);
SELECT COUNT(b) FROM index_test WHERE c IN (125, 700);

SYSTEM FLUSH LOGS;

SELECT COUNT(read_rows) FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%SELECT COUNT%'
  AND type = 'QueryFinish'
  AND read_rows < 1000000;

DROP TABLE index_test;
