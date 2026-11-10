-- https://github.com/ClickHouse/ClickHouse/issues/103069
-- `count_distinct_optimization` rewrites `countDistinct(x)` to
-- `count() FROM (SELECT x GROUP BY x)`. On a Nullable column GROUP BY
-- produces a NULL group that count() would include, while countDistinct
-- / uniqExact skip NULLs. The optimization must not change semantics.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_cd_nullable;
CREATE TABLE t_cd_nullable (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cd_nullable VALUES (NULL)(NULL)(NULL);

SELECT countDistinct(x) FROM t_cd_nullable SETTINGS count_distinct_optimization = 0;
SELECT countDistinct(x) FROM t_cd_nullable SETTINGS count_distinct_optimization = 1;
SELECT uniqExact(x)    FROM t_cd_nullable SETTINGS count_distinct_optimization = 1;

DROP TABLE t_cd_nullable;

DROP TABLE IF EXISTS t_cd_nullable_mix;
CREATE TABLE t_cd_nullable_mix (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cd_nullable_mix VALUES (1)(NULL)(2)(NULL)(1);

SELECT countDistinct(x) FROM t_cd_nullable_mix SETTINGS count_distinct_optimization = 0;
SELECT countDistinct(x) FROM t_cd_nullable_mix SETTINGS count_distinct_optimization = 1;
SELECT uniqExact(x)    FROM t_cd_nullable_mix SETTINGS count_distinct_optimization = 1;

DROP TABLE t_cd_nullable_mix;

DROP TABLE IF EXISTS t_cd_lc_nullable;
CREATE TABLE t_cd_lc_nullable (x LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cd_lc_nullable VALUES ('a')(NULL)('b')(NULL)('a');

SELECT countDistinct(x) FROM t_cd_lc_nullable SETTINGS count_distinct_optimization = 0;
SELECT countDistinct(x) FROM t_cd_lc_nullable SETTINGS count_distinct_optimization = 1;
SELECT uniqExact(x)    FROM t_cd_lc_nullable SETTINGS count_distinct_optimization = 1;

DROP TABLE t_cd_lc_nullable;
