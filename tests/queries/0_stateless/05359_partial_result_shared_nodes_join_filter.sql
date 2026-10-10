-- A filter over a chain of `x + x` aliases is pushed down to the `LEFT JOIN` as one DAG with shared nodes.
-- Checking whether that filter is always false must visit each node once, not every path.

-- JIT compilation names an expression by its full text, which grows exponentially here (issue #123724),
-- and so does planning the first query with `join_use_nulls = 1`.
SELECT count() FROM
(
    SELECT a5_6 + a5_6 AS a6_1, a6_1 + a6_1 AS a6_2, a6_2 + a6_2 AS a6_3, a6_3 + a6_3 AS a6_4, a6_4 + a6_4 AS a6_5, a6_5 + a6_5 AS a6_6 FROM (
    SELECT a4_6 + a4_6 AS a5_1, a5_1 + a5_1 AS a5_2, a5_2 + a5_2 AS a5_3, a5_3 + a5_3 AS a5_4, a5_4 + a5_4 AS a5_5, a5_5 + a5_5 AS a5_6 FROM (
    SELECT a3_6 + a3_6 AS a4_1, a4_1 + a4_1 AS a4_2, a4_2 + a4_2 AS a4_3, a4_3 + a4_3 AS a4_4, a4_4 + a4_4 AS a4_5, a4_5 + a4_5 AS a4_6 FROM (
    SELECT a2_6 + a2_6 AS a3_1, a3_1 + a3_1 AS a3_2, a3_2 + a3_2 AS a3_3, a3_3 + a3_3 AS a3_4, a3_4 + a3_4 AS a3_5, a3_5 + a3_5 AS a3_6 FROM (
    SELECT a1_6 + a1_6 AS a2_1, a2_1 + a2_1 AS a2_2, a2_2 + a2_2 AS a2_3, a2_3 + a2_3 AS a2_4, a2_4 + a2_4 AS a2_5, a2_5 + a2_5 AS a2_6 FROM (
    SELECT r.val + r.val AS a1_1, a1_1 + a1_1 AS a1_2, a1_2 + a1_2 AS a1_3, a1_3 + a1_3 AS a1_4, a1_4 + a1_4 AS a1_5, a1_5 + a1_5 AS a1_6
    FROM (SELECT number AS id FROM numbers(2)) AS l
    LEFT JOIN (SELECT number AS id, number - 1 AS val FROM numbers(1, 1)) AS r ON l.id = r.id)))))
) AS g
INNER JOIN (SELECT 0 AS val) AS k ON g.a6_6 >= k.val
SETTINGS join_use_nulls = 0, compile_expressions = 0, ast_fuzzer_runs = 0;

-- Checking whether an `ANY LEFT JOIN` under such a filter can be converted to a `SEMI` join walks the same DAG.
SELECT count() FROM
(
    SELECT a5_6 + a5_6 AS a6_1, a6_1 + a6_1 AS a6_2, a6_2 + a6_2 AS a6_3, a6_3 + a6_3 AS a6_4, a6_4 + a6_4 AS a6_5, a6_5 + a6_5 AS a6_6 FROM (
    SELECT a4_6 + a4_6 AS a5_1, a5_1 + a5_1 AS a5_2, a5_2 + a5_2 AS a5_3, a5_3 + a5_3 AS a5_4, a5_4 + a5_4 AS a5_5, a5_5 + a5_5 AS a5_6 FROM (
    SELECT a3_6 + a3_6 AS a4_1, a4_1 + a4_1 AS a4_2, a4_2 + a4_2 AS a4_3, a4_3 + a4_3 AS a4_4, a4_4 + a4_4 AS a4_5, a4_5 + a4_5 AS a4_6 FROM (
    SELECT a2_6 + a2_6 AS a3_1, a3_1 + a3_1 AS a3_2, a3_2 + a3_2 AS a3_3, a3_3 + a3_3 AS a3_4, a3_4 + a3_4 AS a3_5, a3_5 + a3_5 AS a3_6 FROM (
    SELECT a1_6 + a1_6 AS a2_1, a2_1 + a2_1 AS a2_2, a2_2 + a2_2 AS a2_3, a2_3 + a2_3 AS a2_4, a2_4 + a2_4 AS a2_5, a2_5 + a2_5 AS a2_6 FROM (
    SELECT r.val + r.val AS a1_1, a1_1 + a1_1 AS a1_2, a1_2 + a1_2 AS a1_3, a1_3 + a1_3 AS a1_4, a1_4 + a1_4 AS a1_5, a1_5 + a1_5 AS a1_6
    FROM (SELECT number AS id FROM numbers(2)) AS l
    ANY LEFT JOIN (SELECT number AS id, number - 1 AS val FROM numbers(1, 1)) AS r ON l.id = r.id)))))
)
WHERE a6_6 >= 0
SETTINGS join_use_nulls = 0, compile_expressions = 0, ast_fuzzer_runs = 0;
