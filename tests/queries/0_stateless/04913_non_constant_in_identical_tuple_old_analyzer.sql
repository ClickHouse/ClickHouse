-- `IN` where the same non-constant tuple appears on both sides, under both analyzers.

SELECT (materialize(1), 2) IN (materialize(1), 2) SETTINGS enable_analyzer = 0;
SELECT (materialize(1), 2) IN (materialize(1), 2) SETTINGS enable_analyzer = 1;

SELECT (2, materialize(1)) IN (2, materialize(1)) SETTINGS enable_analyzer = 0;
SELECT (2, materialize(1)) IN (2, materialize(1)) SETTINGS enable_analyzer = 1;

SELECT (materialize(1), 2, 3) IN (materialize(1), 2, 3) SETTINGS enable_analyzer = 0;
SELECT (materialize(1), 2, 3) IN (materialize(1), 2, 3) SETTINGS enable_analyzer = 1;

SELECT (materialize(1), 'a') IN (materialize(1), 'a') SETTINGS enable_analyzer = 0;
SELECT (materialize(1), 'a') IN (materialize(1), 'a') SETTINGS enable_analyzer = 1;

SELECT (materialize(1), 2) NOT IN (materialize(1), 2) SETTINGS enable_analyzer = 0;
SELECT (materialize(1), 2) NOT IN (materialize(1), 2) SETTINGS enable_analyzer = 1;

SELECT (materialize(1), 2) GLOBAL IN (materialize(1), 2) SETTINGS enable_analyzer = 0;
SELECT (materialize(1), 2) GLOBAL IN (materialize(1), 2) SETTINGS enable_analyzer = 1;

SELECT nullIn((materialize(1), 2), (materialize(1), 2)) SETTINGS enable_analyzer = 0;
SELECT nullIn((materialize(1), 2), (materialize(1), 2)) SETTINGS enable_analyzer = 1;

SELECT (materialize(NULL), NULL) IN (materialize(NULL), NULL) SETTINGS enable_analyzer = 0;
SELECT (materialize(NULL), NULL) IN (materialize(NULL), NULL) SETTINGS enable_analyzer = 1;

SELECT (materialize(NULL), NULL) IN (materialize(NULL), NULL) SETTINGS enable_analyzer = 0, transform_null_in = 1;
SELECT (materialize(NULL), NULL) IN (materialize(NULL), NULL) SETTINGS enable_analyzer = 1, transform_null_in = 1;

SELECT (materialize(toLowCardinality(1)), 2) IN (materialize(toLowCardinality(1)), 2) SETTINGS enable_analyzer = 0;
SELECT (materialize(toLowCardinality(1)), 2) IN (materialize(toLowCardinality(1)), 2) SETTINGS enable_analyzer = 1;

SELECT (materialize(toNullable(1)), 2) IN (materialize(toNullable(1)), 2) SETTINGS enable_analyzer = 0;
SELECT (materialize(toNullable(1)), 2) IN (materialize(toNullable(1)), 2) SETTINGS enable_analyzer = 1;

SELECT (materialize(1), [1, 2]) IN (materialize(1), [1, 2]) SETTINGS enable_analyzer = 0;
SELECT (materialize(1), [1, 2]) IN (materialize(1), [1, 2]) SETTINGS enable_analyzer = 1;

SELECT (materialize(1), 2) IN (materialize(1), 2) SETTINGS enable_analyzer = 0, use_variant_as_common_type = 0;
SELECT (materialize(1), 2) IN (materialize(1), 2) SETTINGS enable_analyzer = 1, use_variant_as_common_type = 0;

SELECT count() FROM numbers(4) WHERE (number, 2) IN (number, 2) SETTINGS enable_analyzer = 0;
SELECT count() FROM numbers(4) WHERE (number, 2) IN (number, 2) SETTINGS enable_analyzer = 1;

SELECT count() FROM numbers(4) WHERE indexHint((number, 2) IN (number, 2)) SETTINGS enable_analyzer = 0;
SELECT count() FROM numbers(4) WHERE indexHint((number, 2) IN (number, 2)) SETTINGS enable_analyzer = 1;

SELECT (materialize(1), (2, 3)) IN (materialize(1), (2, 3)) SETTINGS enable_analyzer = 0; -- { serverError TYPE_MISMATCH }
SELECT (materialize(1), (2, 3)) IN (materialize(1), (2, 3)) SETTINGS enable_analyzer = 1; -- { serverError TYPE_MISMATCH }

SELECT (1, 2) IN (materialize(1), 2) SETTINGS enable_analyzer = 0;
SELECT (1, 2) IN (materialize(1), 2) SETTINGS enable_analyzer = 1;

SELECT (materialize(1), materialize(2)) IN (materialize(1), materialize(2)) SETTINGS enable_analyzer = 0;
SELECT (materialize(1), materialize(2)) IN (materialize(1), materialize(2)) SETTINGS enable_analyzer = 1;

SELECT (materialize(1), 2) IN (materialize(9), 2) SETTINGS enable_analyzer = 0;
SELECT (materialize(1), 2) IN (materialize(9), 2) SETTINGS enable_analyzer = 1;

SELECT (materialize(1), 2) IN (materialize(1), 3) SETTINGS enable_analyzer = 0;
SELECT (materialize(1), 2) IN (materialize(1), 3) SETTINGS enable_analyzer = 1;

SELECT (SELECT (materialize(1), 2) IN (materialize(1), 2)) SETTINGS enable_analyzer = 0;
SELECT (SELECT (materialize(1), 2) IN (materialize(1), 2)) SETTINGS enable_analyzer = 1;

SELECT count() > 0 FROM (
    EXPLAIN PIPELINE graph = 0, graph = 0, compact = 1
    SELECT 'null', (SELECT tuple(materialize(NULL), NULL) GLOBAL IN (materialize(NULL), NULL))
    GROUP BY ALL
) SETTINGS transform_null_in = 1, enable_analyzer = 0;
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE graph = 0, graph = 0, compact = 1
    SELECT 'null', (SELECT tuple(materialize(NULL), NULL) GLOBAL IN (materialize(NULL), NULL))
    GROUP BY ALL
) SETTINGS transform_null_in = 1, enable_analyzer = 1;
