-- ALTER TABLE ... MODIFY QUERY on a materialized view with a TO table keeps the comments of the columns the new query still produces.

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS mv_no_list;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS src;

CREATE TABLE src (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

CREATE MATERIALIZED VIEW mv TO dst (id UInt64 COMMENT 'id comment', v UInt64 COMMENT 'v comment') AS SELECT id, v FROM src;
ALTER TABLE mv MODIFY QUERY SELECT id, v FROM src WHERE id > 0;
SELECT 'same columns', name, type, comment FROM system.columns WHERE database = currentDatabase() AND table = 'mv' ORDER BY position;

-- The comments are in the stored definition too.
DETACH TABLE mv;
ATTACH TABLE mv;
SELECT 'after reload', name, type, comment FROM system.columns WHERE database = currentDatabase() AND table = 'mv' ORDER BY position;

-- A column keeps its comment when its type changes, a new column has none, and a dropped column loses its comment.
ALTER TABLE mv MODIFY QUERY SELECT toString(v) AS v, id + 1 AS w FROM src;
SELECT 'changed columns', name, type, comment FROM system.columns WHERE database = currentDatabase() AND table = 'mv' ORDER BY position;
ALTER TABLE mv MODIFY QUERY SELECT id, v FROM src;
SELECT 'restored columns', name, type, comment FROM system.columns WHERE database = currentDatabase() AND table = 'mv' ORDER BY position;

-- A comment set by ALTER on a view created without a column list.
CREATE MATERIALIZED VIEW mv_no_list TO dst AS SELECT id, v FROM src;
ALTER TABLE mv_no_list COMMENT COLUMN id 'from alter';
ALTER TABLE mv_no_list MODIFY QUERY SELECT id, v FROM src WHERE id > 0;
SELECT 'comment column', name, comment FROM system.columns WHERE database = currentDatabase() AND table = 'mv_no_list' ORDER BY position;

-- In one ALTER, a comment set before MODIFY QUERY is kept and one set after it wins.
-- A Replicated database refuses comment commands mixed with MODIFY QUERY, so this view lives in an Atomic database.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dst (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv TO {CLICKHOUSE_DATABASE_1:Identifier}.dst (id UInt64 COMMENT 'id comment', v UInt64 COMMENT 'v comment') AS SELECT id, v FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mv (COMMENT COLUMN id 'set before'), (MODIFY QUERY SELECT id, v FROM {CLICKHOUSE_DATABASE_1:Identifier}.src WHERE id > 0), (COMMENT COLUMN v 'set after');
SELECT 'same alter', name, comment FROM system.columns WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 'mv' ORDER BY position;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

DROP TABLE mv_no_list;
DROP TABLE mv;
DROP TABLE dst;
DROP TABLE src;
