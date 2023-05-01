SET optimize_move_to_prewhere = 1;

DROP TABLE IF EXISTS t1;

CREATE TABLE t1 ( s String, f Float32, e UInt16 ) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = '100G';

INSERT INTO t1 VALUES ('111', 1, 1);

SELECT s FROM t1 WHERE f AND (e = 1); -- { serverError 59 }
SELECT s FROM t1 PREWHERE f; -- { serverError 59 }
SELECT s FROM t1 PREWHERE f WHERE (e = 1); -- { serverError 59 }
SELECT s FROM t1 PREWHERE f WHERE f AND (e = 1); -- { serverError 59 }

SELECT s FROM t1 WHERE e AND (e = 1);
SELECT s FROM t1 PREWHERE e; -- { serverError 59 }
SELECT s FROM t1 PREWHERE e WHERE (e = 1); -- { serverError 59 }
SELECT s FROM t1 PREWHERE e WHERE f AND (e = 1); -- { serverError 59 }

