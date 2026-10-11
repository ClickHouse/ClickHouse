-- Tags: no-ordinary-database, no-replicated-database
--       A refreshable view needs an Atomic database, and a Replicated one refuses a non-APPEND
--       refreshable view over a plain MergeTree inner table.

SET allow_experimental_refreshable_materialized_view = 1;

DROP TABLE IF EXISTS rmv_comment_src;
DROP TABLE IF EXISTS rmv_comment;

CREATE TABLE rmv_comment_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO rmv_comment_src VALUES (7);

-- A refreshable view has an inner table too, and a non-APPEND refresh replaces that table, so read
-- the comment back after one.
CREATE MATERIALIZED VIEW rmv_comment REFRESH EVERY 1 YEAR (id UInt64 COMMENT 'initial')
    ENGINE = MergeTree ORDER BY id EMPTY AS SELECT id FROM rmv_comment_src;

ALTER TABLE rmv_comment COMMENT COLUMN id 'changed';
SYSTEM REFRESH VIEW rmv_comment;
SYSTEM WAIT VIEW rmv_comment;
SELECT 'refreshed rows', count() FROM rmv_comment;
-- An accepted unrelated ALTER copies the replacement table's columns into the view, so the comment
-- read below comes from that table and not from the view's pre-refresh metadata.
ALTER TABLE rmv_comment MODIFY COMMENT 'view comment';
SELECT 'comment column', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'rmv_comment' AND name = 'id';

DROP TABLE rmv_comment;
DROP TABLE rmv_comment_src;
