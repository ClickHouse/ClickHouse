-- Tags: no-parallel-replicas

-- Tests that the primary key is still used when the predicate uses
-- `LIKE pattern ESCAPE 'c'` or `NOT LIKE pattern ESCAPE 'c'`. The
-- case-insensitive forms `ILIKE ... ESCAPE` and `NOT ILIKE ... ESCAPE` are
-- never pruned by the primary key, with or without an ESCAPE clause.
--
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/105885

SET enable_analyzer = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (s String) ENGINE = MergeTree ORDER BY s
SETTINGS index_granularity = 8192, index_granularity_bytes = 0;

INSERT INTO tab VALUES ('abc'), ('abc%'), ('abc%done'), ('abcd'), ('xyz');

OPTIMIZE TABLE tab FINAL;

SELECT 'Two-argument LIKE: primary key narrows to range [abc, abd)';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT * FROM tab WHERE s LIKE 'abc%'
) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%';

SELECT 'Three-argument LIKE with ESCAPE: escaped % is a literal, so the wildcard-free pattern uses the exact point range [abc%, abc%]';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT * FROM tab WHERE s LIKE 'abc|%' ESCAPE '|'
) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%';

SELECT 'Functional 3-argument form like(col, pattern, escape) is equivalent to the operator form';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT * FROM tab WHERE like(s, 'abc|%', '|')
) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%';

SELECT 'Three-argument NOT LIKE with ESCAPE and a trailing wildcard: primary key narrows to not(in [abc%done, abc%donf))';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT * FROM tab WHERE s NOT LIKE 'abc|%done%' ESCAPE '|'
) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%';

SELECT 'Correctness check: LIKE ... ESCAPE returns rows whose value starts with the literal abc%';

SELECT * FROM tab WHERE s LIKE 'abc|%' ESCAPE '|' ORDER BY s;

SELECT 'Correctness check: NOT LIKE ... ESCAPE excludes rows starting with abc% followed by done';

SELECT * FROM tab WHERE s NOT LIKE 'abc|%done%' ESCAPE '|' ORDER BY s;

SELECT 'A non-ASCII ESCAPE byte is rejected, also by EXPLAIN';

SELECT * FROM tab WHERE like(s, 'abc%', unhex('FF')); -- { serverError BAD_ARGUMENTS }
EXPLAIN indexes = 1 SELECT * FROM tab WHERE like(s, 'abc%', unhex('FF')); -- { serverError BAD_ARGUMENTS }

DROP TABLE tab;

-- `\b` is not an escape sequence, so the pattern matches a literal backslash followed by `b`.
-- ('a\\b01' below is the five-byte string a, backslash, b, 0, 1.)

DROP TABLE IF EXISTS tab2;

CREATE TABLE tab2 (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 2, index_granularity_bytes = 0;

INSERT INTO tab2 VALUES ('a\\b01'), ('a\\b02'), ('a\\b03'), ('a\\b04'), ('abZZ'), ('zzz');

OPTIMIZE TABLE tab2 FINAL;

SELECT 'The primary-key range covers the literal backslash, so matching rows are not pruned';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT * FROM tab2 WHERE s LIKE 'a\\b%' ESCAPE '\\'
) WHERE explain LIKE '%Condition:%';

SELECT 'Correctness check: all four rows starting with a literal backslash are returned';

SELECT * FROM tab2 WHERE s LIKE 'a\\b%' ESCAPE '\\' ORDER BY s;

DROP TABLE tab2;
