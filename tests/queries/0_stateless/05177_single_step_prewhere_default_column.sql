-- https://github.com/ClickHouse/ClickHouse/issues/115273
-- A single-conjunct explicit `PREWHERE` over a column that is itself the condition lost that column:
-- it stayed in the block while the projected-out columns were saved, and was then erased as the filter
-- column. A later chain step could no longer evaluate the `DEFAULT` expression of a column missing
-- from the part, so it was filled with the type default instead.

DROP TABLE IF EXISTS t_single_step_prewhere;
CREATE TABLE t_single_step_prewhere (a UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_single_step_prewhere SELECT 1 FROM numbers(150);
ALTER TABLE t_single_step_prewhere ADD COLUMN b Int32 DEFAULT CAST(a % 13, 'Int32') AFTER a;

SELECT groupUniqArray(b) FROM (SELECT b FROM t_single_step_prewhere WHERE a);
SELECT groupUniqArray(b) FROM (SELECT b FROM t_single_step_prewhere PREWHERE a);
SELECT groupUniqArray(b) FROM (SELECT b FROM t_single_step_prewhere PREWHERE a AND a = 1);
SELECT groupUniqArray((a, b)) FROM (SELECT a, b FROM t_single_step_prewhere PREWHERE a);
SELECT groupUniqArray(b) FROM (SELECT b FROM t_single_step_prewhere PREWHERE a SETTINGS enable_multiple_prewhere_read_steps = 0);
SELECT groupUniqArray(b) FROM (SELECT b FROM t_single_step_prewhere PREWHERE a) SETTINGS enable_analyzer = 0;
DROP TABLE t_single_step_prewhere;

SELECT 'a DEFAULT expression over several columns';
DROP TABLE IF EXISTS t_prewhere_two_deps;
CREATE TABLE t_prewhere_two_deps (a UInt8, c UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prewhere_two_deps SELECT 1, 7 FROM numbers(150);
ALTER TABLE t_prewhere_two_deps ADD COLUMN d Int32 DEFAULT CAST(a + c, 'Int32');
SELECT groupUniqArray(d) FROM (SELECT d FROM t_prewhere_two_deps PREWHERE a);
SELECT groupUniqArray(d) FROM (SELECT d FROM t_prewhere_two_deps PREWHERE c);
SELECT groupUniqArray(d) FROM (SELECT d FROM t_prewhere_two_deps WHERE a);
DROP TABLE t_prewhere_two_deps;

SELECT 'the filter column is still projected out of the result';
DROP TABLE IF EXISTS t_prewhere_projection;
CREATE TABLE t_prewhere_projection (a UInt8, b UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prewhere_projection SELECT number % 2, number FROM numbers(150);
SELECT count(), sum(b) FROM (SELECT b FROM t_prewhere_projection PREWHERE a);
SELECT count(), sum(b) FROM (SELECT b FROM t_prewhere_projection WHERE a);
DROP TABLE t_prewhere_projection;
