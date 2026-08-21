-- Regression test for a null-pointer assertion (px != 0) in TreeRewriter::analyzeSelect.
-- ORDER BY ALL over a Merge table joined with another table under the old analyzer
-- used to abort: removeJoin() dropped the ORDER BY expression but left the
-- order_by_all flag set, so expandOrderByAll() dereferenced a null orderBy().

SET allow_experimental_analyzer = 0;
SET enable_order_by_all = 1;

DROP TABLE IF EXISTS t_merge_join_oba_a;
DROP TABLE IF EXISTS t_merge_join_oba_b;
DROP TABLE IF EXISTS t_merge_join_oba_m;

CREATE TABLE t_merge_join_oba_a (key UInt32) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t_merge_join_oba_b (key UInt32, ID UInt32) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t_merge_join_oba_m (key UInt32) ENGINE = Merge(currentDatabase(), 't_merge_join_oba_a');

INSERT INTO t_merge_join_oba_a VALUES (0);
INSERT INTO t_merge_join_oba_b VALUES (0, 1);

SELECT * FROM t_merge_join_oba_m INNER JOIN t_merge_join_oba_b USING (key) ORDER BY ALL;

-- GROUP BY ALL exercises the symmetric group_by_all flag reset in removeJoin().
SELECT key FROM t_merge_join_oba_m INNER JOIN t_merge_join_oba_b USING (key) GROUP BY ALL ORDER BY ALL;

DROP TABLE t_merge_join_oba_a;
DROP TABLE t_merge_join_oba_b;
DROP TABLE t_merge_join_oba_m;
