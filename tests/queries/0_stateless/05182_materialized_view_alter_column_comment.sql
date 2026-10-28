DROP TABLE IF EXISTS mv_comment_src;
DROP TABLE IF EXISTS mv_comment_inner;
DROP TABLE IF EXISTS mv_comment_two;
DROP TABLE IF EXISTS mv_comment_view;
DROP TABLE IF EXISTS mv_comment_to;
DROP TABLE IF EXISTS mv_comment_target;
DROP TABLE IF EXISTS mv_comment_buffer;
DROP TABLE IF EXISTS mv_comment_dest;

CREATE TABLE mv_comment_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_comment_inner (id UInt64 COMMENT 'initial')
    ENGINE = MergeTree ORDER BY id AS SELECT id FROM mv_comment_src;

ALTER TABLE mv_comment_inner COMMENT COLUMN id 'changed';
SELECT 'comment column', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_inner' AND name = 'id';

ALTER TABLE mv_comment_inner MODIFY COLUMN id COMMENT 'changed again';
-- Read the comment back after a reload, so that the stored CREATE query has to carry it too.
DETACH TABLE mv_comment_inner;
ATTACH TABLE mv_comment_inner;
SELECT 'modify column', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_inner' AND name = 'id';

-- MODIFY QUERY rebuilds the view's columns from the query, which carries no comments; the ones the
-- inner table holds have to come back onto the view.
ALTER TABLE mv_comment_inner MODIFY QUERY SELECT id FROM mv_comment_src WHERE id > 0;
SELECT 'modify query', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_inner' AND name = 'id';

-- A rejected ALTER must not reach the inner table either: a comment left there would surface on the
-- view at its next ALTER. A Replicated database refuses the mixed statement before the storage sees
-- it, hence the second error code.
ALTER TABLE mv_comment_inner COMMENT COLUMN id 'not stored', MODIFY QUERY SELECT id, 1 AS extra FROM mv_comment_src; -- { serverError NO_SUCH_COLUMN_IN_TABLE, QUERY_IS_PROHIBITED }
ALTER TABLE mv_comment_inner MODIFY COMMENT 'unrelated';
SELECT 'after rejected alter', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_inner' AND name = 'id';

-- An empty comment is a comment too: it has to reach the inner table and win on the view.
ALTER TABLE mv_comment_inner COMMENT COLUMN id '';
SELECT 'cleared comment', empty(comment) FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_inner' AND name = 'id';

-- Only the column the statement names takes its comment from the view; the other keeps the inner
-- table's, also through the MODIFY QUERY that rebuilds the view's columns without any.
CREATE MATERIALIZED VIEW mv_comment_two (a UInt64 COMMENT 'a0', b UInt64 COMMENT 'b0')
    ENGINE = MergeTree ORDER BY a AS SELECT id AS a, id AS b FROM mv_comment_src;
ALTER TABLE mv_comment_two COMMENT COLUMN a 'a1';
SELECT 'two columns', name, comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_two' ORDER BY name;
ALTER TABLE mv_comment_two MODIFY QUERY SELECT id AS a, id AS b FROM mv_comment_src WHERE id > 0;
SELECT 'two columns after modify query', name, comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_two' ORDER BY name;

-- A regular view has no inner table, so nothing replaces its column descriptions.
CREATE VIEW mv_comment_view (id UInt64 COMMENT 'initial') AS SELECT id FROM mv_comment_src;
ALTER TABLE mv_comment_view COMMENT COLUMN id 'changed';
SELECT 'plain view', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_view' AND name = 'id';

-- A view with a TO clause has no inner table either, and the explicit target is a table of its own:
-- the comment belongs on the view, and the target keeps the comment it was created with.
CREATE TABLE mv_comment_target (id UInt64 COMMENT 'target') ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_comment_to TO mv_comment_target AS SELECT id FROM mv_comment_src;
ALTER TABLE mv_comment_to COMMENT COLUMN id 'changed';
SELECT 'view with TO', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_to' AND name = 'id';
SELECT 'TO target', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_target' AND name = 'id';

-- A comment command that `IF EXISTS` turns into a no-op must not reach the inner table either: an
-- inner engine's own ALTER carries its own side effects, and `Buffer` flushes its rows on every one.
CREATE TABLE mv_comment_dest (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_comment_buffer
    ENGINE = Buffer(currentDatabase(), mv_comment_dest, 1, 1000, 1000, 1000, 1000000, 10000000, 100000000)
    AS SELECT id FROM mv_comment_src;
INSERT INTO mv_comment_src VALUES (1);
ALTER TABLE mv_comment_buffer COMMENT COLUMN IF EXISTS no_such_column 'ignored';
SELECT 'ignored comment command', count() FROM mv_comment_dest;

DROP TABLE mv_comment_buffer;
DROP TABLE mv_comment_dest;
DROP TABLE mv_comment_to;
DROP TABLE mv_comment_target;
DROP TABLE mv_comment_view;
DROP TABLE mv_comment_two;
DROP TABLE mv_comment_inner;
DROP TABLE mv_comment_src;
