-- An outer `LIMIT n AFTER/UNTIL` range counts rows from the boundary row onwards, so its count must
-- not be pushed into a plain view as an ordinary `LIMIT n`: the view would keep only its first n rows
-- and the boundary row might never reach the range. The rows are compared as sorted sets, because the
-- order in which `groupArray` receives them depends on how many threads run the aggregation.
DROP TABLE IF EXISTS t_outer_range_src;
DROP VIEW IF EXISTS v_outer_range_plain;

CREATE TABLE t_outer_range_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_outer_range_src SELECT number FROM numbers(10);
CREATE VIEW v_outer_range_plain AS SELECT x FROM t_outer_range_src;

SELECT arraySort(groupArray(x)) FROM (SELECT x FROM v_outer_range_plain ORDER BY x LIMIT 2 AFTER x >= 5) SETTINGS enable_analyzer = 1;
SELECT arraySort(groupArray(x)) FROM (SELECT x FROM v_outer_range_plain ORDER BY x LIMIT 2 UNTIL x >= 8) SETTINGS enable_analyzer = 1;
SELECT arraySort(groupArray(x)) FROM (SELECT x FROM v_outer_range_plain ORDER BY x LIMIT 1 AFTER x % 4 = 3 ALL) SETTINGS enable_analyzer = 1;
SELECT arraySort(groupArray(x)) FROM (SELECT x FROM v_outer_range_plain ORDER BY x DESC LIMIT 2 AFTER x <= 5) SETTINGS enable_analyzer = 1;

SELECT arraySort(groupArray(x)) FROM (SELECT x FROM v_outer_range_plain ORDER BY x LIMIT 2 AFTER x >= 5) SETTINGS enable_analyzer = 0;
SELECT arraySort(groupArray(x)) FROM (SELECT x FROM v_outer_range_plain ORDER BY x LIMIT 2 UNTIL x >= 8) SETTINGS enable_analyzer = 0;
SELECT arraySort(groupArray(x)) FROM (SELECT x FROM v_outer_range_plain ORDER BY x LIMIT 1 AFTER x % 4 = 3 ALL) SETTINGS enable_analyzer = 0;
SELECT arraySort(groupArray(x)) FROM (SELECT x FROM v_outer_range_plain ORDER BY x DESC LIMIT 2 AFTER x <= 5) SETTINGS enable_analyzer = 0;

DROP VIEW v_outer_range_plain;
DROP TABLE t_outer_range_src;
