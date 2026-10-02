-- An alias is not allowed in PARTITION BY, PRIMARY KEY, ORDER BY, UNIQUE KEY, TTL or a skip index
-- of a new table, or in ALTER TABLE ... MODIFY ORDER BY, MODIFY TTL and ADD INDEX.

DROP TABLE IF EXISTS t_key_alias;
DROP TABLE IF EXISTS t_sample_alias;
DROP TABLE IF EXISTS mv_key_alias;
DROP TABLE IF EXISTS t_src;

CREATE TABLE t_key_alias (c0 Int64) ENGINE = MergeTree PRIMARY KEY (c0 AS a); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64) ENGINE = MergeTree ORDER BY c0 PRIMARY KEY (c0 AS a); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64, PRIMARY KEY (c0 AS a)) ENGINE = MergeTree; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64) ENGINE = MergeTree ORDER BY c0 * 2 PRIMARY KEY (c0 AS a) * 2; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64, c1 Int64) ENGINE = MergeTree ORDER BY (c0 AS x, c1); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64, c1 Int64) ENGINE = MergeTree PARTITION BY (c1 AS p) ORDER BY c0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64, c1 Int64) ENGINE = MergeTree ORDER BY c0 UNIQUE KEY (c1 AS u); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64, d Date) ENGINE = MergeTree ORDER BY c0 TTL (d + INTERVAL 1 DAY AS x) RECOMPRESS CODEC(ZSTD(1)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64, d Date) ENGINE = MergeTree ORDER BY c0 TTL d + INTERVAL 1 DAY WHERE (c0 > 1 AS a); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64, c1 Int64, d Date) ENGINE = MergeTree ORDER BY c0 TTL d + INTERVAL 1 DAY GROUP BY c0 AS x SET c1 = max(c1); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64, c1 Int64, d Date) ENGINE = MergeTree ORDER BY c0 TTL d + INTERVAL 1 DAY GROUP BY c0 SET c1 = max(c1 AS m); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64, d Date) ENGINE = MergeTree ORDER BY c0 TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(1 AS l)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64, c1 Int64, INDEX i (c1 AS a) TYPE minmax) ENGINE = MergeTree ORDER BY c0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_key_alias (c0 Int64, c1 Int64, INDEX i (c1 * 1000 AS c0) TYPE minmax) ENGINE = MergeTree ORDER BY c0; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_src (x Int64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_key_alias ENGINE = MergeTree PRIMARY KEY (x AS a) AS SELECT x FROM t_src; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('t_key_alias', 'mv_key_alias');

CREATE TABLE t_key_alias (c0 Int64, c1 Int64) ENGINE = MergeTree PARTITION BY c1 PRIMARY KEY c0 ORDER BY (c0, c1);
INSERT INTO t_key_alias VALUES (1, 1), (2, 2);
SELECT count() FROM t_key_alias;

ALTER TABLE t_key_alias MODIFY ORDER BY ((c0, c1) AS x); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_key_alias MODIFY ORDER BY (c0 AS x, c1); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_key_alias ADD COLUMN c2 Int64, MODIFY ORDER BY (c0, c1, c2 AS x); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_key_alias ADD COLUMN c2 Int64, MODIFY ORDER BY (c0, c1, (c2 AS x) + 1); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_key_alias ADD COLUMN c2 Int64, MODIFY ORDER BY (c0, c1, c2), MODIFY ORDER BY (c0, c1, c2 AS x); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_key_alias MODIFY TTL (toDate(c0) + INTERVAL 1 DAY AS x); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_key_alias MODIFY TTL toDate(c0) + INTERVAL 1 DAY GROUP BY c0 SET c1 = max(c1 AS m); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_key_alias ADD INDEX i (c1 AS a) TYPE minmax; -- { serverError BAD_ARGUMENTS }
CREATE INDEX i ON t_key_alias (c1 * 2 AS a) TYPE minmax; -- { serverError BAD_ARGUMENTS }
CREATE HYPOTHETICAL INDEX h ON t_key_alias (c1 AS a) TYPE minmax; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_key_alias ADD COLUMN c2 Int64, MODIFY ORDER BY (c0, c1, c2);
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_key_alias';

-- SAMPLE BY is not checked.
CREATE TABLE t_sample_alias (c0 UInt64) ENGINE = MergeTree ORDER BY c0 SAMPLE BY (c0 AS s);
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_sample_alias';

DROP TABLE t_sample_alias;
DROP TABLE t_key_alias;
DROP TABLE t_src;
