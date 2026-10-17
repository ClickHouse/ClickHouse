-- Dropping an `Alias` with `IF EMPTY` removes only the alias, which holds no rows of its own, so it
-- is not judged by the target's row count, even when the target's row count is unknown. `TRUNCATE`
-- of an alias truncates the target, so `TRUNCATE ... IF EMPTY` is judged by the target.

DROP TABLE IF EXISTS t_alias_if_empty_mt;
DROP TABLE IF EXISTS t_alias_if_empty_file;
DROP TABLE IF EXISTS a_alias_if_empty_mt;
DROP TABLE IF EXISTS a_alias_if_empty_file;

CREATE TABLE t_alias_if_empty_mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_alias_if_empty_mt VALUES (1), (2), (3);
CREATE TABLE t_alias_if_empty_file (x UInt64) ENGINE = File(TSV);
INSERT INTO t_alias_if_empty_file VALUES (1);

CREATE TABLE a_alias_if_empty_mt ENGINE = Alias('t_alias_if_empty_mt');
CREATE TABLE a_alias_if_empty_file ENGINE = Alias('t_alias_if_empty_file');

TRUNCATE TABLE IF EMPTY a_alias_if_empty_mt; -- { serverError TABLE_NOT_EMPTY }
SELECT count() FROM t_alias_if_empty_mt;

DROP TABLE IF EMPTY a_alias_if_empty_mt SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EMPTY a_alias_if_empty_file SETTINGS ignore_drop_queries_probability = 0;

SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '%alias_if_empty%' ORDER BY name;
SELECT count() FROM t_alias_if_empty_mt;
SELECT count() FROM t_alias_if_empty_file;

DROP TABLE t_alias_if_empty_mt;
DROP TABLE t_alias_if_empty_file;
