-- Tags: shard

-- A constant LowCardinality argument is not counted as a full LowCardinality
-- argument when the result type is computed, so the result of concat() is wrapped
-- in LowCardinality. Over a distributed query the constness of an aggregation key
-- is dropped in the merging-aggregator header, so two non-constant LowCardinality
-- columns reach the default LowCardinality function implementation. This used to
-- throw LOGICAL_ERROR "Default functions implementation for LowCardinality is
-- supported only with a single LowCardinality argument".

DROP TABLE IF EXISTS t_04489;
CREATE TABLE t_04489 (d Dynamic) ENGINE = Memory;
INSERT INTO t_04489 VALUES (1), (2), ('a'), ('b'), (NULL);

SELECT concatAssumeInjective((SELECT toLowCardinality('p')), dynamicType(d)) AS type, count()
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04489)
GROUP BY GROUPING SETS ((type)) ORDER BY type;

SELECT concat((SELECT toLowCardinality('p')), dynamicType(d)) AS type, count()
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04489)
GROUP BY GROUPING SETS ((type)) ORDER BY type;

SELECT concatAssumeInjective((SELECT toLowCardinality(toNullable('p'))), dynamicType(d)) AS type
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04489)
GROUP BY GROUPING SETS ((type)) ORDER BY type;

DROP TABLE t_04489;
