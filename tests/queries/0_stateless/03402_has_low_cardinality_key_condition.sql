-- Regression test: has() with LowCardinality column in ORDER BY with CAST
-- caused "Bad cast from ColumnVector to ColumnLowCardinality" exception
-- in applyDeterministicDagToColumn when building key condition for has().
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=97568&sha=9785d6ea5d327f0a7671c45dada310eb44e63044&name_0=PR&name_1=AST%20fuzzer%20%28amd_msan%29

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_has_lc;

CREATE TABLE t_has_lc (d LowCardinality(DateTime('America/New_York')))
ENGINE = MergeTree ORDER BY CAST(d, 'String') SETTINGS index_granularity = 1;

INSERT INTO t_has_lc VALUES (toDateTime(1730611800, 'America/New_York')), (toDateTime(1730615400, 'America/New_York'));

SELECT toUnixTimestamp(d) FROM t_has_lc WHERE has([toDateTime(1730611800, 'America/New_York')], d);

DROP TABLE t_has_lc;


DROP TABLE IF EXISTS t_nested2;
CREATE TABLE t_nested2
(
    d Array(LowCardinality(DateTime('America/New_York')))
)
ENGINE = MergeTree
ORDER BY CAST(d, 'Array(DateTime(\'America/New_York\'))')
SETTINGS index_granularity = 1;

INSERT INTO t_nested2 VALUES ([toDateTime(1730611800, 'America/New_York')]);

SELECT toUnixTimestamp(d[1])
FROM t_nested2
WHERE d = CAST(
    [toDateTime(1730611800, 'America/New_York')],
    'Array(LowCardinality(DateTime(\'America/New_York\')))'
);
