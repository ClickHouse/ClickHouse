SELECT formatQueryFromJSON(parseQueryToJSON(
    'CREATE TABLE t (x UInt64, PROJECTION p (x CODEC(DoubleDelta, ZSTD)) AS (SELECT x ORDER BY x)) ENGINE = MergeTree ORDER BY x'))
    = formatQuerySingleLine(
    'CREATE TABLE t (x UInt64, PROJECTION p (x CODEC(DoubleDelta, ZSTD)) AS (SELECT x ORDER BY x)) ENGINE = MergeTree ORDER BY x');

SELECT formatQueryFromJSON(parseQueryToJSON(
    'CREATE TABLE t (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x, count() GROUP BY x)) ENGINE = MergeTree ORDER BY x'))
    = formatQuerySingleLine(
    'CREATE TABLE t (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x, count() GROUP BY x)) ENGINE = MergeTree ORDER BY x');

SELECT position(formatQuerySingleLine(
    'CREATE TABLE t (select UInt64, PROJECTION p (select CODEC(NONE)) AS (SELECT select ORDER BY select)) ENGINE = MergeTree ORDER BY select'),
    'PROJECTION p (`select` CODEC(NONE)) AS') > 0;
SELECT formatQueryFromJSON(
    '{"type":"ProjectionDeclaration","name":"p","columns":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]},"query":{"type":"ProjectionSelectQuery","select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]}}}'); -- { serverError BAD_ARGUMENTS }

WITH
    parseQueryToJSON('CREATE TABLE t (x UInt64, y UInt64, PROJECTION p (x CODEC(NONE), y CODEC(ZSTD)) AS (SELECT x, y ORDER BY x)) ENGINE = MergeTree ORDER BY x') AS original,
    replaceOne(
        original,
        '"order_by":{"type":"Identifier","name":"x"}},"columns":{"type":"ExpressionList"',
        '"order_by":{"type":"Identifier","name":"x"}},"columns":{"type":"ExpressionList","separator":";"') AS malformed
SELECT formatQueryFromJSON(malformed); -- { serverError BAD_ARGUMENTS }
