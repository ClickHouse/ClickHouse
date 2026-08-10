-- `used_number_of_joins` counts the physical joins of the executed pipeline, so it is checked
-- with both analyzers: they build the join steps in different places.

SET log_queries = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS tj;

CREATE TABLE t1 (a UInt64) ENGINE = Memory;
CREATE TABLE t2 (a UInt64) ENGINE = Memory;
CREATE TABLE t3 (a UInt64) ENGINE = Memory;
CREATE TABLE tj (a UInt64, b UInt64) ENGINE = Join(ANY, LEFT, a);

INSERT INTO t1 SELECT number FROM numbers(10);
INSERT INTO t2 SELECT number FROM numbers(10);
INSERT INTO t3 SELECT number FROM numbers(10);
INSERT INTO tj SELECT number, number FROM numbers(10);

-- No join at all.
SELECT count() FROM t1 FORMAT Null SETTINGS log_comment = '04654_join_count_none_new', enable_analyzer = 1;
SELECT count() FROM t1 FORMAT Null SETTINGS log_comment = '04654_join_count_none_old', enable_analyzer = 0;

-- A single INNER JOIN.
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04654_join_count_inner_new', enable_analyzer = 1;
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a FORMAT Null SETTINGS log_comment = '04654_join_count_inner_old', enable_analyzer = 0;

-- Three tables, so two physical joins.
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.a = t3.a FORMAT Null SETTINGS log_comment = '04654_join_count_two_new', enable_analyzer = 1;
SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.a = t3.a FORMAT Null SETTINGS log_comment = '04654_join_count_two_old', enable_analyzer = 0;

-- CROSS JOIN.
SELECT count() FROM t1, t2 FORMAT Null SETTINGS log_comment = '04654_join_count_cross_new', enable_analyzer = 1;
SELECT count() FROM t1, t2 FORMAT Null SETTINGS log_comment = '04654_join_count_cross_old', enable_analyzer = 0;

-- A table of the Join engine is joined by `FilledJoinStep` and not by `JoinStep`.
SELECT count() FROM t1 ANY LEFT JOIN tj USING (a) FORMAT Null SETTINGS log_comment = '04654_join_count_filled_new', enable_analyzer = 1;
SELECT count() FROM t1 ANY LEFT JOIN tj USING (a) FORMAT Null SETTINGS log_comment = '04654_join_count_filled_old', enable_analyzer = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, used_number_of_joins
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '04654\_join\_count\_%'
ORDER BY log_comment;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE tj;
