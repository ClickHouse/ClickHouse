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
DROP TABLE mv_comment_src;
