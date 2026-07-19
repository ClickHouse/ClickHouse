-- A stateful function inside a subquery that wraps a join must keep observing the same rows and
-- blocks as written. `optimizeJoin` may flatten an `ExpressionStep` wrapping a child join into the
-- global join graph and reattach it at the final reordered join (`query_plan_merge_expression_into_join`
-- is enabled by default). Without the `hasStatefulFunctions()` fence, `neighbor` in the inner join's
-- projection would be floated above the outer join and evaluated on only the surviving rows.
--
-- Here `neighbor(v, 1)` is computed over the full 8-row `l INNER JOIN m` (values 0..7, so the shifted
-- values are 1..7,0), and only keys 0 and 1 survive the join with `r` -> [1, 2]. If the expression is
-- reattached above the 3-way join it sees only the 2 surviving rows -> [1, 0].

DROP TABLE IF EXISTS l_04559;
DROP TABLE IF EXISTS m_04559;
DROP TABLE IF EXISTS r_04559;

CREATE TABLE l_04559 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO l_04559 SELECT number, number FROM numbers(8);

CREATE TABLE m_04559 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO m_04559 SELECT number FROM numbers(8);

CREATE TABLE r_04559 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO r_04559 SELECT number FROM numbers(2);

SET allow_deprecated_error_prone_window_functions = 1;
SET query_plan_merge_expression_into_join = 1;
SET max_threads = 1;
SET max_block_size = 65536;
SET join_algorithm = 'hash';
SET enable_parallel_replicas = 0;

SET enable_analyzer = 1;
SELECT groupArray(n) FROM (SELECT neighbor(v, 1) AS n, l_04559.k AS k FROM l_04559 INNER JOIN m_04559 ON l_04559.k = m_04559.k) lm INNER JOIN r_04559 ON lm.k = r_04559.k;

SET enable_analyzer = 0;
SELECT groupArray(n) FROM (SELECT neighbor(v, 1) AS n, l_04559.k AS k FROM l_04559 INNER JOIN m_04559 ON l_04559.k = m_04559.k) lm INNER JOIN r_04559 ON lm.k = r_04559.k;

DROP TABLE l_04559;
DROP TABLE m_04559;
DROP TABLE r_04559;
