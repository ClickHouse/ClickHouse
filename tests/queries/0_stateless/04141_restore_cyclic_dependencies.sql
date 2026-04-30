-- Tags: no-fasttest
-- Test that RESTORE does not crash when the dependency graph contains cyclic dependencies.
-- See https://github.com/ClickHouse/ClickHouse/issues/103746

DROP DATABASE IF EXISTS db_04141 SYNC;
CREATE DATABASE db_04141;

CREATE TABLE db_04141.src (id Int32) ENGINE = MergeTree() ORDER BY id;

-- mv1 has a CTE named `mv2` whose name collides with the real table mv2,
-- creating a false cycle in the dependency graph: mv1 -> mv2 (CTE) AND mv2 -> mv1 (real).
CREATE MATERIALIZED VIEW db_04141.mv1 (id Int32) ENGINE = MergeTree() ORDER BY id
    AS WITH mv2 AS (SELECT id FROM db_04141.src) SELECT * FROM mv2;

CREATE MATERIALIZED VIEW db_04141.mv2 (id Int32) ENGINE = MergeTree() ORDER BY id
    AS SELECT id FROM db_04141.mv1;

BACKUP DATABASE db_04141 TO Memory('backup_04141') FORMAT Null;

DROP DATABASE db_04141 SYNC;

-- Before the fix this would abort the server due to SIZE_MAX overflow in
-- TablesDependencyGraph::getTablesSplitByDependencyLevel.
RESTORE DATABASE db_04141 FROM Memory('backup_04141') FORMAT Null;

SELECT 'server alive';

DROP DATABASE IF EXISTS db_04141 SYNC;
