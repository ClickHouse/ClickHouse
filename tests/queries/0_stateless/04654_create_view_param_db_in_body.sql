-- Tags: need-query-parameters

-- An ordinary view keeps the query-parameter placeholders of its SELECT body unsubstituted: they
-- are the view's parameterizable interface, resolved when the view is called. Rebuilding such an
-- identifier as a resolved table name aborted on `!part.empty()` in debug and sanitizer builds.

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.src (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.src VALUES (1, 'own');

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (1, 'other');

USE {CLICKHOUSE_DATABASE:Identifier};

-- The database of a table in the body comes from a parameter (rebuilt by `DDLDependencyVisitor`).
-- Calling the view twice with different databases shows the target is only known at call time,
-- which is why no dependency on any one table may be recorded at create time.
CREATE VIEW v_from AS SELECT v FROM {pdb:Identifier}.src;
SELECT 'from', (SELECT v FROM v_from(pdb = {CLICKHOUSE_DATABASE:String})), (SELECT v FROM v_from(pdb = {CLICKHOUSE_DATABASE_1:String}));

-- The same identifier one level down, in a JOIN, and in a UNION ALL branch.
CREATE VIEW v_subquery AS SELECT v FROM (SELECT v FROM {pdb:Identifier}.src);
SELECT 'subquery', (SELECT v FROM v_subquery(pdb = {CLICKHOUSE_DATABASE_1:String}));

CREATE VIEW v_join AS SELECT b.v AS v FROM src AS a JOIN {pdb:Identifier}.src AS b ON a.k = b.k;
SELECT 'join', (SELECT v FROM v_join(pdb = {CLICKHOUSE_DATABASE_1:String}));

CREATE VIEW v_union AS SELECT v FROM src UNION ALL SELECT v FROM {pdb:Identifier}.src;
SELECT 'union', (SELECT count() FROM v_union(pdb = {CLICKHOUSE_DATABASE_1:String}));

-- An explicit column list also reaches the dependency scan.
CREATE VIEW v_columns (v String) AS SELECT v FROM {pdb:Identifier}.src;
SELECT 'columns', (SELECT v FROM v_columns(pdb = {CLICKHOUSE_DATABASE_1:String}));

-- RENAME re-runs the dependency scan over the stored body.
RENAME TABLE v_columns TO v_renamed;
SELECT 'renamed', (SELECT v FROM v_renamed(pdb = {CLICKHOUSE_DATABASE_1:String}));

-- The right-hand side of IN is rebuilt by `AddDefaultDatabaseVisitor`.
CREATE TABLE keys (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO keys VALUES (1);
CREATE VIEW v_in AS SELECT v FROM src WHERE k IN {ptab:Identifier};
SELECT 'in', (SELECT v FROM v_in(ptab = 'keys'));

-- A `dictGet` name is qualified by `AddDefaultDatabaseVisitor` without going through `createTable`.
CREATE TABLE dsrc (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO dsrc VALUES (1, 'looked_up');
CREATE DICTIONARY dict (id UInt64, val String) PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'dsrc')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);
CREATE VIEW v_dictget AS SELECT dictGet({pdict:Identifier}, 'val', k) AS g FROM src;
SELECT 'dictget', (SELECT g FROM v_dictget(pdict = 'dict'));

-- A view over a resolvable name must keep recording its referential dependency, so its source
-- cannot be dropped. This is the liveness control: it must stay refused after the fix.
CREATE TABLE lit (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE VIEW v_literal AS SELECT k FROM lit;
DROP TABLE lit SETTINGS check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP VIEW v_literal;
DROP TABLE lit SETTINGS check_referential_table_dependencies = 1;
SELECT 'literal dependency enforced, released with the view';

-- A parameterized body must NOT record a dependency on the same-named table of the current
-- database: the parameter can name any database, so that edge guards the wrong table. Before the
-- fix this DROP failed with HAVE_DEPENDENT_OBJECTS.
CREATE TABLE tgt (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE VIEW v_param AS SELECT k FROM {pdb:Identifier}.tgt;
DROP TABLE tgt SETTINGS check_referential_table_dependencies = 1;
SELECT 'parameterized body records no dependency on the current database';

-- A MATERIALIZED view is not a parameterized view, so its body IS substituted at create time,
-- while an ordinary view keeps the placeholder. That asymmetry is why only ordinary views reach
-- the rebuild, and it must be preserved: an over-broad fix would blur the two.
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY v AS SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;
SELECT 'materialized body substituted', position(create_table_query, '{CLICKHOUSE_DATABASE_1') = 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'mv';
SELECT 'ordinary body keeps placeholder', position(create_table_query, '{pdb') > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'v_from';

-- A parameter in the TABLE position stays an error and must not become an abort.
CREATE VIEW v_bad AS SELECT v FROM {CLICKHOUSE_DATABASE:Identifier}.{ptab:Identifier}; -- { serverError UNKNOWN_TABLE }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
