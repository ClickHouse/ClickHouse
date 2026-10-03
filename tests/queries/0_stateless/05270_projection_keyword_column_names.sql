-- A projection column named `select` or `with` must not be mistaken for a query.
SELECT position(formatQuerySingleLine('CREATE TABLE t (select UInt64, PROJECTION p (select CODEC(NONE)) AS (SELECT select ORDER BY select)) ENGINE = MergeTree ORDER BY select'), 'PROJECTION p (`select` CODEC(NONE)) AS') > 0;
SELECT position(formatQuerySingleLine('CREATE TABLE t (with UInt64, PROJECTION p (with CODEC(NONE)) AS (SELECT with ORDER BY with)) ENGINE = MergeTree ORDER BY with'), 'PROJECTION p (`with` CODEC(NONE)) AS') > 0;
SELECT position(formatQuerySingleLine('ALTER TABLE t ADD PROJECTION p (select CODEC(NONE)) AS (SELECT select ORDER BY select)'), 'PROJECTION p (`select` CODEC(NONE)) AS') > 0;

-- The query-only forms must still parse as queries.
SELECT position(formatQuerySingleLine('CREATE TABLE t (x UInt64, PROJECTION p (SELECT x ORDER BY x)) ENGINE = MergeTree ORDER BY x'), 'PROJECTION p (SELECT x ORDER BY x)') > 0;
SELECT position(formatQuerySingleLine('CREATE TABLE t (x UInt64, PROJECTION p (WITH x AS y SELECT y ORDER BY y)) ENGINE = MergeTree ORDER BY x'), 'PROJECTION p (WITH') > 0;
