-- A common table expression is expanded before the database is filled into a mutation command, so a
-- table identifier hidden by an expression alias of the same name is left unqualified. With
-- `enable_global_with_statement` disabled the alias is not visible in the subquery, so the identifier
-- names a table there and has to be resolved in the database of the updated table, not of the session.
-- The source table exists in both databases with a different row, so an expression resolved in the
-- database of the session silently reads the wrong row instead of failing.
-- The reference is one `SELECT` deeper than the alias: only a lookup in an enclosing scope is
-- disabled, so a reference in the select that declares the alias reads it either way.
-- The old analyzer resolves a common table expression in a subquery of a mutation as a table, so the
-- analyzer is requested explicitly, as in `04693_merge_table_function_in_mutation`.

CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (99);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (2);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.u (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.u VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (8, 0), (99, 0),
    (21, 0), (22, 0), (23, 0), (24, 0), (25, 0), (26, 0), (27, 0), (28, 0);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 0), (2, 0), (3, 0), (99, 0);

-- Every row starts at 0 and every expected value is non-zero, so a statement that does not update the
-- row at all also fails. The row `id = 99` exists so that a predicate resolved in the database of the
-- session updates the wrong row rather than no row.

-- An assignment reads the table of the updated database (2), not the table of the session (99).
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0))
    WHERE id = 1 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

-- The `ALTER TABLE ... UPDATE` spelling of the same expression answers the same.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0))
    WHERE id = 1 SETTINGS mutations_sync = 2, enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- The predicate is expanded by a separate call, so it is asserted separately: it marks the row the
-- updated database names (2), not the one the session database names (99).
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = 11
    WHERE id IN (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0))
    SETTINGS enable_analyzer = 1;
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE v = 11;

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = 11
    WHERE id IN (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0))
    SETTINGS mutations_sync = 2, enable_analyzer = 1;
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE v = 11;

-- The effective value of the setting is read, so a profile that carries it answers the same as the
-- setting written out.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS compatibility = '20.3'))
    WHERE id = 3 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 3;

-- A clause is read per arm of a union: the arm carrying it names a table there, resolved in the
-- updated database (2), while the arm without it keeps reading the alias (7 * 1000). The two arms
-- are weighted differently so that reading the clause for the wrong arm, for both arms or for
-- neither answers 2007, 2002 and 7007 rather than the expected value.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH src AS (SELECT 7 AS id) SELECT sum(m) FROM (
                 SELECT max(id) * 1000 AS m FROM src
                 UNION ALL
                 SELECT max(id) AS m FROM src SETTINGS enable_global_with_statement = 0))
    WHERE id = 8 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 8;

-- With the setting at its default the alias IS visible in the subquery, so the same reference reads
-- the common table expression (7) and must not be qualified as a table.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src))
    WHERE id = 4 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 4;

-- A reference in the select that declares the alias keeps reading the alias, and the table its body
-- names is resolved in the updated database (2).
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH c AS (SELECT max(id) AS m FROM src) SELECT m FROM c)
    WHERE id = 5 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 5;

-- An identifier that no alias hides was always resolved in the updated database (2).
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (SELECT max(id) FROM src)
    WHERE id = 6 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 6;

-- The same expansion carries a `WITH` expression alias down, and such an alias is visible in a nested
-- select in one more way than a common table expression: with `enable_scopes_for_with_statement`
-- disabled the declaring select's aliases are copied into every descendant scope, and a select reads
-- that copy when it disables the setting too. So the alias is visible either when the nested select
-- enables `enable_global_with_statement`, or when the declaring and the reading select both disable
-- `enable_scopes_for_with_statement`; the two mixed combinations hide it. The value below encodes
-- which name won: 100 = the array of the alias, 10 = the table of the updated database,
-- 1 = the table of the session database, 0 = the row was not updated.

-- The alias is hidden in the subquery, so the identifier names a table, resolved in the updated
-- database (10).
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH [7] AS src
        SELECT (SELECT toUInt8(7 IN src) * 100 + toUInt8(2 IN src) * 10 + toUInt8(99 IN src)
                SETTINGS enable_global_with_statement = 0))
    WHERE id = 21 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 21;

-- Both selects disable `enable_scopes_for_with_statement`, so the reading one resolves the copy and
-- the array is still substituted (100). Dropping every inherited alias instead answers 10.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH [7] AS src
        SELECT (SELECT toUInt8(7 IN src) * 100 + toUInt8(2 IN src) * 10 + toUInt8(99 IN src)
                SETTINGS enable_global_with_statement = 0))
    WHERE id = 22 SETTINGS enable_analyzer = 1, enable_scopes_for_with_statement = 0;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 22;

-- Mixed: the declaring select disables the scopes and the reading one keeps them, so the copy is not
-- read and the alias is hidden (10). Reading the setting of the declaring select alone answers 100.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH [7] AS src
        SELECT (SELECT toUInt8(7 IN src) * 100 + toUInt8(2 IN src) * 10 + toUInt8(99 IN src)
                SETTINGS enable_global_with_statement = 0, enable_scopes_for_with_statement = 1))
    WHERE id = 23 SETTINGS enable_analyzer = 1, enable_scopes_for_with_statement = 0;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 23;

-- The other mixed combination: the reading select disables the scopes and the declaring one does
-- not, so no copy was made and the alias is hidden (10). Reading the setting of the reading select
-- alone answers 100.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH [7] AS src
        SELECT (SELECT toUInt8(7 IN src) * 100 + toUInt8(2 IN src) * 10 + toUInt8(99 IN src)
                SETTINGS enable_global_with_statement = 0, enable_scopes_for_with_statement = 0))
    WHERE id = 24 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 24;

-- Nothing anywhere is named `lit`, so a hidden alias leaves an identifier that resolves to no table
-- and the statement is rejected, as the same expression is when run as a plain `SELECT`, instead of
-- silently reading the substituted array.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH [5] AS lit SELECT (SELECT toUInt8(5 IN lit) SETTINGS enable_global_with_statement = 0))
    WHERE id = 25 SETTINGS enable_analyzer = 1; -- { serverError UNKNOWN_IDENTIFIER }

-- The copy reaches a select through an intermediate one that does not read it, so the alias is
-- hidden in the middle select and visible again in the innermost (100). An implementation that
-- pruned the inherited aliases in place, instead of keeping the copy apart from them, answers 10.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH [7] AS src
        SELECT (SELECT (SELECT toUInt8(7 IN src) * 100 + toUInt8(2 IN src) * 10 + toUInt8(99 IN src)
                        SETTINGS enable_global_with_statement = 0, enable_scopes_for_with_statement = 0)
                SETTINGS enable_global_with_statement = 0, enable_scopes_for_with_statement = 1))
    WHERE id = 26 SETTINGS enable_analyzer = 1, enable_scopes_for_with_statement = 0;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 26;

-- A nearer declaration made with the scopes enabled does not shadow the copy, so the outer alias
-- wins (100) rather than the nearer one (1000).
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH [7] AS src
        SELECT (WITH [5] AS src
            SELECT (SELECT toUInt8(5 IN src) * 1000 + toUInt8(7 IN src) * 100 + toUInt8(2 IN src) * 10
                           + toUInt8(99 IN src)
                    SETTINGS enable_scopes_for_with_statement = 0)
            SETTINGS enable_scopes_for_with_statement = 1))
    WHERE id = 27 SETTINGS enable_analyzer = 1, enable_scopes_for_with_statement = 0;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 27;

-- The predicate is expanded by its own call, so the alias carrier is asserted there too: with the
-- alias hidden the identifier names the table of the updated database, which holds the row the
-- predicate looks for, and the row is updated. Substituting the array instead makes the conjunct
-- false and leaves the row at 0.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = 28
    WHERE id = 28 AND (WITH [7] AS src
        SELECT (SELECT toUInt8(2 IN src) SETTINGS enable_global_with_statement = 0))
    SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 28;

DROP TABLE src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
