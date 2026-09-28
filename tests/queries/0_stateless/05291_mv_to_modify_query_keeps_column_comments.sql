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

DROP TABLE mv_no_list;
DROP TABLE mv;
DROP TABLE dst;
DROP TABLE src;
