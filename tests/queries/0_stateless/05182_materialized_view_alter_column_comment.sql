DROP TABLE IF EXISTS mv_comment_src;
DROP TABLE IF EXISTS mv_comment_inner;
DROP TABLE IF EXISTS mv_comment_view;

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

-- A rejected ALTER must not reach the inner table either: a comment left there would surface on the
-- view at its next ALTER. A Replicated database refuses the mixed statement before the storage sees
-- it, hence the second error code.
ALTER TABLE mv_comment_inner COMMENT COLUMN id 'not stored', MODIFY QUERY SELECT id, 1 AS extra FROM mv_comment_src; -- { serverError NO_SUCH_COLUMN_IN_TABLE, QUERY_IS_PROHIBITED }
ALTER TABLE mv_comment_inner MODIFY COMMENT 'unrelated';
SELECT 'after rejected alter', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_inner' AND name = 'id';

-- A regular view has no inner table, so nothing replaces its column descriptions.
CREATE VIEW mv_comment_view (id UInt64 COMMENT 'initial') AS SELECT id FROM mv_comment_src;
ALTER TABLE mv_comment_view COMMENT COLUMN id 'changed';
SELECT 'plain view', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_comment_view' AND name = 'id';

DROP TABLE mv_comment_view;
DROP TABLE mv_comment_inner;
DROP TABLE mv_comment_src;
