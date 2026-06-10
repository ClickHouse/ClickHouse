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

SELECT 'A non-ASCII ESCAPE byte is rejected at planning time by the primary-key analyzer';

-- Mirrors the execution-layer validation in `FunctionsStringSearch::executeImpl`:
-- a single non-ASCII byte is not a valid ESCAPE, so the predicate must be rejected
-- by `KeyCondition::extractAtomFromTree` before any optimization can drop it.
SELECT * FROM tab WHERE like(s, 'abc%', unhex('FF')); -- { serverError BAD_ARGUMENTS }
EXPLAIN indexes = 1 SELECT * FROM tab WHERE like(s, 'abc%', unhex('FF')); -- { serverError BAD_ARGUMENTS }

DROP TABLE tab;

-- An unknown backslash escape keeps the literal backslash at row level, but
-- `extractFixedPrefixFromLikePattern` drops it and would build the prefix range for `ab`,
-- skipping the part that holds the literal `a\b...` (which sorts below `ab`). The analyzer
-- must decline the key condition for such patterns and fall back to row-level evaluation.
-- ('a\\b01' is the literal four-byte string a, backslash, b, 0, 1.)

DROP TABLE IF EXISTS tab2;

CREATE TABLE tab2 (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 2;

INSERT INTO tab2 VALUES ('a\\b01'), ('a\\b02'), ('a\\b03'), ('a\\b04'), ('abZZ'), ('zzz');

OPTIMIZE TABLE tab2 FINAL;

SELECT 'Unknown backslash escape: primary key declines (Condition: true) so matching rows are not pruned';

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT * FROM tab2 WHERE s LIKE 'a\\b%' ESCAPE '\\'
) WHERE explain LIKE '%Condition:%';

SELECT 'Correctness check: all four rows starting with a literal backslash are returned';

SELECT * FROM tab2 WHERE s LIKE 'a\\b%' ESCAPE '\\' ORDER BY s;

DROP TABLE tab2;
