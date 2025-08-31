DROP TEMPORARY TABLE IF EXISTS tview_basic;
DROP TEMPORARY TABLE IF EXISTS t_src;

CREATE TEMPORARY TABLE t_src (id UInt32, val String) ENGINE = Memory;
INSERT INTO t_src VALUES (1,'a'), (2,'b'), (3,'c');

CREATE TEMPORARY VIEW tview_basic AS
SELECT id, upper(val) AS u
FROM t_src
WHERE id <= 2;

SELECT * FROM tview_basic ORDER BY id;

CREATE TEMPORARY VIEW IF NOT EXISTS tview_basic AS SELECT 0;

-- CREATE OR REPLACE TEMPORARY VIEW tview_basic AS SELECT 0; -- { serverError 62 }

CREATE TEMPORARY VIEW default.tview_db AS SELECT 1; -- { serverError 442 }

CREATE TEMPORARY VIEW tview_cluster ON CLUSTER 'test' AS SELECT 1; -- { serverError 80 }

DROP TEMPORARY TABLE IF EXISTS tview_basic;
DROP TEMPORARY TABLE IF EXISTS t_src;