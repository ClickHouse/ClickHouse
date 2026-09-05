-- `ParserTableOverridesDeclarationList` fills `ASTTableOverrideList` only through `setTableOverride`
-- with standalone overrides parsed as `TABLE OVERRIDE name (...)`, so every parser-produced child is
-- standalone, has a non-empty name, and names never repeat (`setTableOverride` replaces an existing
-- entry instead of appending). `ParserExplainQuery` parses `EXPLAIN TABLE OVERRIDE` with
-- `ParserTableOverrideDeclaration(false)`, so its override is always the embedded form: not
-- standalone and without a table name. Shapes violating these invariants would format as SQL the
-- parser can never produce (bare `PARTITION BY` after `CREATE DATABASE ...`, or
-- `EXPLAIN TABLE OVERRIDE <function> TABLE OVERRIDE name ...`), or make `tryGetTableOverride` see
-- only the last duplicate while formatting emits all of them. Reject them at the deserialization
-- boundary.

-- Parser-produced shapes round-trip byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE DATABASE db ENGINE = MaterializedMySQL(''127.0.0.1:3306'', ''db'', ''user'', ''pw'') TABLE OVERRIDE t1 (PARTITION BY a), TABLE OVERRIDE t2 (COLUMNS (b Int32))'));
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN TABLE OVERRIDE mysql(''127.0.0.1:3306'', ''db'', ''table'', ''user'', ''pw'') PARTITION BY toYYYYMM(created)'));

-- A non-standalone child in a `TableOverrideList` is parser-impossible.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE DATABASE db ENGINE = MaterializedMySQL(''127.0.0.1:3306'', ''db'', ''user'', ''pw'') TABLE OVERRIDE t1 (PARTITION BY a)'),
    '"table_name":"t1","is_standalone":true',
    '"table_name":"t1","is_standalone":false')); -- { serverError BAD_ARGUMENTS }

-- An empty table name is parser-impossible (`ParserIdentifier` requires a name).
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE DATABASE db ENGINE = MaterializedMySQL(''127.0.0.1:3306'', ''db'', ''user'', ''pw'') TABLE OVERRIDE t1 (PARTITION BY a)'),
    '"table_name":"t1"',
    '"table_name":""')); -- { serverError BAD_ARGUMENTS }

-- Duplicate names are parser-impossible: `setTableOverride` replaces instead of appending.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE DATABASE db ENGINE = MaterializedMySQL(''127.0.0.1:3306'', ''db'', ''user'', ''pw'') TABLE OVERRIDE t1 (PARTITION BY a), TABLE OVERRIDE t2 (COLUMNS (b Int32))'),
    '"table_name":"t2"',
    '"table_name":"t1"')); -- { serverError BAD_ARGUMENTS }

-- A standalone override attached to `EXPLAIN TABLE OVERRIDE` is parser-impossible.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('EXPLAIN TABLE OVERRIDE mysql(''127.0.0.1:3306'', ''db'', ''table'', ''user'', ''pw'') PARTITION BY toYYYYMM(created)'),
    '"table_name":"","is_standalone":false',
    '"table_name":"","is_standalone":true')); -- { serverError BAD_ARGUMENTS }

-- A named override attached to `EXPLAIN TABLE OVERRIDE` is parser-impossible too.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('EXPLAIN TABLE OVERRIDE mysql(''127.0.0.1:3306'', ''db'', ''table'', ''user'', ''pw'') PARTITION BY toYYYYMM(created)'),
    '"table_name":"","is_standalone":false',
    '"table_name":"t1","is_standalone":false')); -- { serverError BAD_ARGUMENTS }
