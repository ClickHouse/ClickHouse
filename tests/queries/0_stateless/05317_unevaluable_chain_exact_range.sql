-- Index analysis answers "unknown" for a range on which a monotonic function chain cannot be evaluated.
-- Such an answer is an over-approximation and must not be reported as an inconsistency by the exact range
-- search of `count()` (it was a `LOGICAL_ERROR` "Inconsistent KeyCondition behavior" in debug builds).

DROP TABLE IF EXISTS t_unevaluable_chain;

CREATE TABLE t_unevaluable_chain (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_unevaluable_chain VALUES (-9223372036854775808), (-1), (0), (1), (2);

-- The query itself divides the minimal signed number by minus one, so it fails - but with the error of the function.
SELECT count() FROM t_unevaluable_chain WHERE intDiv(a, toInt16(-1)) IN (0); -- { serverError ILLEGAL_DIVISION }
SELECT count() FROM t_unevaluable_chain WHERE intDiv(a, toInt16(-1)) = 0; -- { serverError ILLEGAL_DIVISION }

-- After the offending row is deleted, the index still holds it, and the query must succeed.
DELETE FROM t_unevaluable_chain WHERE a = -9223372036854775808;
SELECT count() FROM t_unevaluable_chain WHERE intDiv(a, toInt16(-1)) IN (0);
SELECT count() FROM t_unevaluable_chain WHERE intDiv(a, toInt16(-1)) = 0;

DROP TABLE t_unevaluable_chain;
