SET enable_analyzer = 1;
SET query_plan_merge_filter_into_join_condition = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (a UInt64) ENGINE = Log;
CREATE TABLE r (a UInt64) ENGINE = Log;

INSERT INTO l SELECT number % 16 FROM numbers(500);
INSERT INTO r SELECT number % 16 FROM numbers(500);

-- A comma join with `rand` must keep the equality in `WHERE`.
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r WHERE rand(l.a) % 16 = r.a AND rand(l.a) % 16 = 3
SETTINGS cross_to_inner_join_rewrite = 1;

-- An explicit `CROSS` join must keep the equality in `WHERE` too.
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l CROSS JOIN r WHERE rand(l.a) % 16 = r.a AND rand(l.a) % 16 = 3
SETTINGS cross_to_inner_join_rewrite = 1;

DROP TABLE l;
DROP TABLE r;
