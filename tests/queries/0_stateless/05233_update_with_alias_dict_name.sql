-- The dictionary name of `dictGet` is one of the places a `WITH` expression alias is substituted
-- into by a standalone `UPDATE`. A subquery that does not look into an enclosing scope cannot see
-- the alias, so the name is a dictionary name there, as it is in the equivalent `SELECT`.
-- The old analyzer resolves the alias differently, so the analyzer is requested explicitly.

DROP DICTIONARY IF EXISTS d;
DROP DICTIONARY IF EXISTS other;
DROP TABLE IF EXISTS d_src;
DROP TABLE IF EXISTS other_src;
DROP TABLE IF EXISTS u;

CREATE TABLE d_src (k UInt64, val String) ENGINE = MergeTree ORDER BY k;
INSERT INTO d_src VALUES (1, 'from_d');
CREATE TABLE other_src (k UInt64, val String) ENGINE = MergeTree ORDER BY k;
INSERT INTO other_src VALUES (1, 'from_other');

CREATE DICTIONARY d (k UInt64, val String) PRIMARY KEY k
    SOURCE(CLICKHOUSE(TABLE 'd_src')) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY other (k UInt64, val String) PRIMARY KEY k
    SOURCE(CLICKHOUSE(TABLE 'other_src')) LAYOUT(FLAT()) LIFETIME(0);

CREATE TABLE u (id UInt64, v String) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO u VALUES (1, ''), (2, ''), (3, '');

-- `from_other` says the alias won, `from_d` says the dictionary of that name won.

-- The alias is visible in the subquery by default, so the dictionary it names is read.
UPDATE u SET v = (WITH 'other' AS d SELECT (SELECT dictGet(d, 'val', toUInt64(1))))
    WHERE id = 1 SETTINGS enable_analyzer = 1;
SELECT v FROM u WHERE id = 1;

-- The subquery does not look into an enclosing scope, so `d` is a dictionary name there.
UPDATE u SET v = (WITH 'other' AS d
        SELECT (SELECT dictGet(d, 'val', toUInt64(1)) SETTINGS enable_global_with_statement = 0))
    WHERE id = 2 SETTINGS enable_analyzer = 1;
SELECT v FROM u WHERE id = 2;

-- With the scopes disabled the declaring select's aliases are copied into every descendant scope, so
-- a subquery that disables them too reads the alias again.
UPDATE u SET v = (WITH 'other' AS d
        SELECT (SELECT dictGet(d, 'val', toUInt64(1)) SETTINGS enable_global_with_statement = 0))
    WHERE id = 3 SETTINGS enable_analyzer = 1, enable_scopes_for_with_statement = 0;
SELECT v FROM u WHERE id = 3;

DROP DICTIONARY d;
DROP DICTIONARY other;
DROP TABLE d_src;
DROP TABLE other_src;
DROP TABLE u;
