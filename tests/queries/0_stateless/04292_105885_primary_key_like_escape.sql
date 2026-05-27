-- Tests that the primary key (`KeyCondition`) is still used when the predicate
-- uses `LIKE pattern ESCAPE 'c'` or `NOT LIKE pattern ESCAPE 'c'`. The escape
-- character is folded into the pattern (rewritten to standard backslash
-- escapes) before `KeyCondition::extractAtomFromTree` dispatches it through
-- the existing 2-argument handler. `ILIKE ... ESCAPE` and `NOT ILIKE ... ESCAPE`
-- are not pruned by the primary key because `ilike`/`notILike` are not in
-- `KeyCondition::atom_map`; case-insensitive forms fall back to row-level
-- evaluation. The text index covers all four forms (see test 04277).
--
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/105885

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (s String) ENGINE = MergeTree ORDER BY s;

INSERT INTO tab VALUES ('abc'), ('abc%'), ('abc%done'), ('abcd'), ('xyz');

OPTIMIZE TABLE tab FINAL;

SELECT 'Two-argument LIKE: primary key narrows to range [abc, abd)';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT * FROM tab WHERE s LIKE 'abc%'
) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%';

SELECT 'Three-argument LIKE with ESCAPE: literal % folded into prefix, range becomes [abc%, abc&)';

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

DROP TABLE tab;
