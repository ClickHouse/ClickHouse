DROP TABLE IF EXISTS ttl_on_insert;
DROP TABLE IF EXISTS ttl_on_insert_where;

CREATE TABLE ttl_on_insert (d DateTime, x UInt32)
ENGINE = MergeTree ORDER BY x PARTITION BY toYYYYMM(d)
TTL d + INTERVAL 1 DAY
SETTINGS remove_empty_parts = 0;

SYSTEM STOP MERGES ttl_on_insert;

SELECT '-- optimize_on_insert = 1: expired rows are not written, one part for the live rows';

SET optimize_on_insert = 1;
INSERT INTO ttl_on_insert VALUES ('2000-01-01 00:00:00', 1), ('2000-02-01 00:00:00', 2), (now() + INTERVAL 1 YEAR, 3), (now() + INTERVAL 1 YEAR, 4);

SELECT x FROM ttl_on_insert ORDER BY x;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_on_insert' AND active;

SELECT '-- a block of only expired rows writes no part';

INSERT INTO ttl_on_insert VALUES ('2001-01-01 00:00:00', 5), ('2001-02-01 00:00:00', 6);

SELECT count() FROM ttl_on_insert;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_on_insert' AND active;

SELECT '-- optimize_on_insert = 0: expired rows are written and wait for a merge';

SET optimize_on_insert = 0;
INSERT INTO ttl_on_insert VALUES ('2002-01-01 00:00:00', 7), ('2002-02-01 00:00:00', 8);

SELECT x FROM ttl_on_insert ORDER BY x;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_on_insert' AND active;

SYSTEM START MERGES ttl_on_insert;
OPTIMIZE TABLE ttl_on_insert FINAL;
SELECT x FROM ttl_on_insert ORDER BY x;

SELECT '-- TTL DELETE WHERE is not applied on insert';

SET optimize_on_insert = 1;
CREATE TABLE ttl_on_insert_where (d DateTime, x UInt32)
ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 DAY DELETE WHERE x % 2 = 0;

INSERT INTO ttl_on_insert_where VALUES ('2000-01-01 00:00:00', 1), ('2000-01-01 00:00:00', 2);

SELECT x FROM ttl_on_insert_where ORDER BY x;

DROP TABLE ttl_on_insert;
DROP TABLE ttl_on_insert_where;
