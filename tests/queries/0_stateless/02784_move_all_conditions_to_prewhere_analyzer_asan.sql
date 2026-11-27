DROP TABLE IF EXISTS t_02784;

CREATE TABLE t_02784 (c1 UInt64, c2 UInt64) ENGINE=MergeTree() ORDER BY c1 SETTINGS min_bytes_for_wide_part=1;

INSERT INTO t_02784 SELECT number, number FROM numbers(1);

SET enable_analyzer=1;
SET move_all_conditions_to_prewhere=1;

SELECT c1, c2 FROM t_02784 WHERE c1 = 0 AND c2 = 0;
SELECT c1, c2 FROM t_02784 WHERE c2 = 0 AND c1 = 0;
SELECT c2, c1 FROM t_02784 WHERE c1 = 0 AND c2 = 0;
SELECT c2, c1 FROM t_02784 WHERE c2 = 0 AND c1 = 0;

DROP TABLE t_02784;
