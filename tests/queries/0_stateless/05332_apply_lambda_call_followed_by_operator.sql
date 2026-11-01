-- `APPLY lambda(...)` followed by an operator: the call is the lambda, the operator applies to its result.
SELECT * APPLY lambda(tuple(x), toString(x)) > '0' FROM (SELECT 1 AS a);
SELECT * APPLY lambda(tuple(x), [x, x + 1])[2] FROM (SELECT 1 AS a);
SELECT COLUMNS('a') APPLY lambda(tuple(x), x * 10) - 1 FROM (SELECT 2 AS a);
SELECT t.* APPLY lambda(tuple(x), x * 10) + 1 FROM (SELECT 2 AS a) AS t;
SELECT * EXCEPT b REPLACE (a + 1 AS a) APPLY lambda(tuple(x), x * 10) > 15 FROM (SELECT 1 AS a, 2 AS b);
SELECT * APPLY lambda(tuple(x), (x, x + 1)).2 FROM (SELECT 1 AS a);
SELECT * APPLY lambda(tuple(x), x + 1)::String FROM (SELECT 1 AS a);
-- Only the second `APPLY` is followed by an operator.
SELECT * APPLY lambda(tuple(x), x * 2) APPLY lambda(tuple(x), x + 1) > 2 FROM (SELECT 1 AS a);
SELECT * APPLY (lambda(tuple(x), toString(x))) > '0' FROM (SELECT 1 AS a);
-- A view stores `APPLY (lambda(...))` without the brackets and parses it back on ATTACH.
CREATE VIEW v_apply AS SELECT * APPLY (lambda(tuple(x), toString(x))) > '0' AS r FROM (SELECT 1 AS a);
DETACH VIEW v_apply;
ATTACH TABLE v_apply;
SELECT * FROM v_apply;
DROP VIEW v_apply;
SELECT * APPLY lambda(x, y) > 1; -- { clientError SYNTAX_ERROR }
-- Only the plain call is the lambda: parameters, NULLS modifiers and OVER stay a syntax error.
SELECT * APPLY lambda(tuple(x), sum(x)) OVER () > 0 FROM (SELECT 1 AS a); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda(tuple(x), any(x)) IGNORE NULLS > 0 FROM (SELECT 1 AS a); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda(1)(tuple(x), x + 1) > 0 FROM (SELECT 1 AS a); -- { clientError SYNTAX_ERROR }

-- In a codec the operator is formatted as a function call, so nothing follows the `APPLY` text there.
CREATE TABLE t_codec (v Int8 CODEC(ZSTD(* APPLY lambda(tuple(x), toString(x)) > 1))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_codec (v Int8 CODEC(ZSTD(* APPLY lambda(tuple(x), toString(x))[1]))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }
