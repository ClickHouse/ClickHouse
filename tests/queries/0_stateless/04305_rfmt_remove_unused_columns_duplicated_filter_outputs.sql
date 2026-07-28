SET enable_analyzer = 1;
SET query_plan_remove_unused_columns = 1;

DROP ROW POLICY IF EXISTS p_04305 ON t_04305;
DROP TABLE IF EXISTS t_04305;

CREATE TABLE t_04305
(
    a UInt8,
    b UInt8
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_04305 VALUES (0, 10), (1, 20), (2, 30);

SELECT 'prewhere';
SELECT materialize(a) AS a
FROM t_04305
PREWHERE a
ORDER BY a;

SELECT 'row policy';
CREATE ROW POLICY p_04305 ON t_04305 USING a TO ALL;

SELECT materialize(a) AS a
FROM t_04305
ORDER BY a;

SELECT 'row policy bare column analyzer on';
SELECT a FROM t_04305 ORDER BY a SETTINGS enable_analyzer = 1;
SELECT a, sum(b) FROM t_04305 GROUP BY a ORDER BY a SETTINGS enable_analyzer = 1;

SELECT 'row policy bare column analyzer off';
SELECT a FROM t_04305 ORDER BY a SETTINGS enable_analyzer = 0;
SELECT a, sum(b) FROM t_04305 GROUP BY a ORDER BY a SETTINGS enable_analyzer = 0;

DROP ROW POLICY p_04305 ON t_04305;
DROP TABLE t_04305;
