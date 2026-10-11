SELECT formatQueryFromJSON(parseQueryToJSON(
    'CREATE TABLE t (x UInt64, PROJECTION p (x CODEC(DoubleDelta, ZSTD)) AS (SELECT x ORDER BY x)) ENGINE = MergeTree ORDER BY x'))
    = formatQuerySingleLine(
    'CREATE TABLE t (x UInt64, PROJECTION p (x CODEC(DoubleDelta, ZSTD)) AS (SELECT x ORDER BY x)) ENGINE = MergeTree ORDER BY x');

SELECT position(formatQueryFromJSON(parseQueryToJSON(
    'CREATE TABLE t (x UInt64, PROJECTION p (SELECT x ORDER BY x)) ENGINE = MergeTree ORDER BY x')),
    'PROJECTION p') > 0;

-- JSON reconstruction must also preserve the older projection index shape.
SELECT formatQueryFromJSON(parseQueryToJSON(
    'CREATE TABLE t (x UInt64, PROJECTION p INDEX x TYPE minmax) ENGINE = MergeTree ORDER BY x'))
    = formatQuerySingleLine(
    'CREATE TABLE t (x UInt64, PROJECTION p INDEX x TYPE minmax) ENGINE = MergeTree ORDER BY x');
SELECT formatQueryFromJSON(parseQueryToJSON(
    'CREATE TABLE t (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x, count() GROUP BY x)) ENGINE = MergeTree ORDER BY x'))
    = formatQuerySingleLine(
    'CREATE TABLE t (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x, count() GROUP BY x)) ENGINE = MergeTree ORDER BY x');

SELECT position(formatQuerySingleLine(
    'CREATE TABLE t (select UInt64, PROJECTION p (select CODEC(NONE)) AS (SELECT select ORDER BY select)) ENGINE = MergeTree ORDER BY select'),
    'PROJECTION p (`select` CODEC(NONE)) AS') > 0;
SELECT position(formatQuerySingleLine(
    'CREATE TABLE t (x UInt64, PROJECTION p (WITH x AS y SELECT y ORDER BY y)) ENGINE = MergeTree ORDER BY x'),
    'PROJECTION p (WITH') > 0;

SELECT formatQueryFromJSON(
    '{"type":"ProjectionDeclaration","name":"p","columns":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]},"query":{"type":"ProjectionSelectQuery"}}'); -- { serverError BAD_ARGUMENTS }

WITH
    parseQueryToJSON('CREATE TABLE t (x UInt64, y UInt64, PROJECTION p (x CODEC(NONE), y CODEC(ZSTD)) AS (SELECT x, y ORDER BY x)) ENGINE = MergeTree ORDER BY x') AS original,
    replaceOne(
        original,
        '"order_by":{"type":"Identifier","name":"x"}},"columns":{"type":"ExpressionList"',
        '"order_by":{"type":"Identifier","name":"x"}},"columns":{"type":"ExpressionList","separator":";"') AS malformed
SELECT formatQueryFromJSON(malformed); -- { serverError BAD_ARGUMENTS }

-- The projection SELECT list must retain the parser's non-empty, comma-separated shape.
WITH
    parseQueryToJSON('CREATE TABLE t (x UInt64, PROJECTION p (SELECT x ORDER BY x)) ENGINE = MergeTree ORDER BY x') AS original,
    replaceOne(original, '"select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]}',
        '"select":{"type":"ExpressionList","children":[]}') AS malformed
SELECT formatQueryFromJSON(malformed); -- { serverError BAD_ARGUMENTS }

-- A raw query node cannot occupy a projection INDEX expression slot.
WITH
    parseQueryToJSON('CREATE TABLE t (x UInt64, PROJECTION p INDEX x TYPE commit_order) ENGINE = MergeTree ORDER BY x') AS original,
    JSONExtractRaw(parseQueryToJSON('SELECT 1'), 'list_of_selects', 'children', 1) AS raw_query,
    replaceOne(
        original,
        '"index":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]}',
        concat('"index":{"type":"ExpressionList","children":[', raw_query, ']}')) AS malformed
SELECT formatQueryFromJSON(malformed); -- { serverError BAD_ARGUMENTS }
