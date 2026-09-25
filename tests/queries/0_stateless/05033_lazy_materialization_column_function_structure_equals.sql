-- https://github.com/ClickHouse/ClickHouse/issues/122043
-- Captured-lambda ColumnFunction must support structureEquals so LazyMaterializingTransform can squash main-path chunks.

DROP TABLE IF EXISTS t_lazy_cf;
CREATE TABLE t_lazy_cf (c1 String, c2 DateTime64(6), c3 String) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t_lazy_cf SELECT leftPad(toString(number), 8, '0'), toDateTime64('2026-08-01 00:00:00', 6) + number, if(number % 2000 = 0, 'x ab', 'zz') FROM numbers(1000000);

SELECT count()
FROM
(
    SELECT c1, arrayFirst(s -> endsWith(upper(c3), s), ['AB', 'CD']) AS c4
    FROM t_lazy_cf
    WHERE arrayExists(s -> endsWith(upper(c3), s), ['AB', 'CD'])
    ORDER BY c2 DESC
    LIMIT 1000
);

SELECT count()
FROM
(
    SELECT c1, arrayFirst(s -> endsWith(upper(c3), s), ['AB', 'CD']) AS c4
    FROM t_lazy_cf
    WHERE arrayExists(s -> endsWith(upper(c3), s), ['AB', 'CD'])
    ORDER BY c2 DESC
    LIMIT 1000
    SETTINGS query_plan_optimize_lazy_materialization = 0
);

DROP TABLE t_lazy_cf;
