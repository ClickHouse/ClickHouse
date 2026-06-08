DROP TABLE IF EXISTS t_update_prewhere;

SET mutations_sync = 2;
SET apply_mutations_on_fly = 0;

CREATE TABLE t_update_prewhere (id UInt64, c1 UInt64, c2 UInt64, c3 UInt64)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_update_prewhere SELECT number, number, number, number FROM numbers(100000);

ALTER TABLE t_update_prewhere UPDATE c1 = 0 WHERE id % 2 = 0;

SELECT count() FROM t_update_prewhere PREWHERE c1 != 0 WHERE c2 % 3 = 0;

ALTER TABLE t_update_prewhere UPDATE c3 = c2 * 2 WHERE c1 != 0;

SELECT sum(c3) FROM t_update_prewhere PREWHERE c3 % 3 = 0 WHERE c1 != 0;

ALTER TABLE t_update_prewhere UPDATE c2 = c3 - c1  WHERE c3 < 10000;

SELECT sum(c2) FROM t_update_prewhere PREWHERE c1 != 0 WHERE c2 % 5 = 1;

DROP TABLE IF EXISTS t_update_prewhere;

SELECT '++++++++++++++++++++';

SET mutations_sync = 0;
SET apply_mutations_on_fly = 1;

CREATE TABLE t_update_prewhere (id UInt64, c1 UInt64, c2 UInt64, c3 UInt64)
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES t_update_prewhere;

INSERT INTO t_update_prewhere SELECT number, number, number, number FROM numbers(100000);

ALTER TABLE t_update_prewhere UPDATE c1 = 0 WHERE id % 2 = 0;

SELECT count() FROM t_update_prewhere PREWHERE c1 != 0 WHERE c2 % 3 = 0;

ALTER TABLE t_update_prewhere UPDATE c3 = c2 * 2 WHERE c1 != 0;

SELECT sum(c3) FROM t_update_prewhere PREWHERE c3 % 3 = 0 WHERE c1 != 0;

ALTER TABLE t_update_prewhere UPDATE c2 = c3 - c1  WHERE c3 < 10000;

SELECT sum(c2) FROM t_update_prewhere PREWHERE c1 != 0 WHERE c2 % 5 = 1;

DROP TABLE IF EXISTS t_update_prewhere;
