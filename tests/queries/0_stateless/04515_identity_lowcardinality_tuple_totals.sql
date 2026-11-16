-- The Bad-cast abort only happens with the analyzer, and the declared tuple type name
-- differs between analyzers (named vs unnamed), so pin the analyzer for a stable reference.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_04515;
CREATE TABLE t_04515 (key UInt8, value LowCardinality(String)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_04515 SELECT number % 3, toString(number) FROM numbers(100);

-- identity() returns its argument column verbatim, so its result type must equal the argument type.
-- Previously the default LowCardinality implementation stripped (nested) LowCardinality from the declared
-- type while the passthrough column kept it, aborting during WITH TOTALS serialization of a constant key.
SELECT toTypeName(identity((SELECT toString(1), toLowCardinality(''))));
SELECT toTypeName(identity(toLowCardinality('x'))), identity(toLowCardinality('x'));

-- Regression: must not abort with 'Bad cast from type ColumnLowCardinality to ColumnString'.
SELECT identity((SELECT toString(1), toLowCardinality(''))) AS k, count(*) FROM t_04515 GROUP BY ALL WITH TOTALS ORDER BY k;
SELECT identity((SELECT toString(1), toLowCardinality(''))) AS k, count(*) FROM t_04515 GROUP BY ALL WITH ROLLUP ORDER BY k;

DROP TABLE t_04515;
