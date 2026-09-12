-- Tags: no-old-analyzer
--       no-old-analyzer: the old analyzer never resolves a subcolumn of an ALIAS parent.

DROP TABLE IF EXISTS mv_comment_src;
DROP TABLE IF EXISTS mv_comment_inner;

CREATE TABLE mv_comment_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_comment_inner (id UInt64 COMMENT 'initial')
    ENGINE = MergeTree ORDER BY id AS SELECT id FROM mv_comment_src;

SELECT 'created', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_inner' AND name = 'id';

ALTER TABLE mv_comment_inner COMMENT COLUMN id 'changed';
SELECT 'comment column', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_inner' AND name = 'id';

ALTER TABLE mv_comment_inner MODIFY COLUMN id COMMENT 'changed again';
SELECT 'modify column', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_inner' AND name = 'id';

-- An unrelated accepted alter must not revert the column comment.
ALTER TABLE mv_comment_inner MODIFY COMMENT 'view comment';
SELECT 'after table comment', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_inner' AND name = 'id';

DROP TABLE mv_comment_inner;

-- An ALIAS column has no real subcolumns of its own: they are derived from its expression, so
-- restoring a comment must not register them.
CREATE MATERIALIZED VIEW mv_comment_alias (id UInt64, arr Array(UInt64) ALIAS [id])
    ENGINE = MergeTree ORDER BY id AS SELECT id FROM mv_comment_src;
INSERT INTO mv_comment_src VALUES (7);
ALTER TABLE mv_comment_alias MODIFY COMMENT 'view comment';
SELECT 'alias subcolumn', arr.size0 FROM mv_comment_alias;

ALTER TABLE mv_comment_alias COMMENT COLUMN arr 'alias comment';
SELECT 'alias comment', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_alias' AND name = 'arr';
SELECT 'alias subcolumn again', arr.size0 FROM mv_comment_alias;

DROP TABLE mv_comment_alias;
DROP TABLE mv_comment_src;
