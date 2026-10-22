-- Tags: no-replicated-database
-- `COMMENT COLUMN` advances the `validate()` snapshot so a later
-- `MODIFY COLUMN ... REMOVE COMMENT` in the same ALTER sees the comment.
-- no-replicated-database: the statement mixes a comment command with a replicated alter,
-- which Replicated databases prohibit.

DROP TABLE IF EXISTS comment_then_remove;
CREATE TABLE comment_then_remove (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;

ALTER TABLE comment_then_remove
    COMMENT COLUMN x 'c',
    MODIFY COLUMN x REMOVE COMMENT;

SELECT 'comment then remove', name, comment FROM system.columns
    WHERE database = currentDatabase() AND table = 'comment_then_remove' AND name = 'x';

-- A later `COMMENT COLUMN` overwrites the working snapshot, so REMOVE still succeeds.
ALTER TABLE comment_then_remove
    COMMENT COLUMN x 'first',
    COMMENT COLUMN x 'second',
    MODIFY COLUMN x REMOVE COMMENT;

SELECT 'comment twice then remove', name, comment FROM system.columns
    WHERE database = currentDatabase() AND table = 'comment_then_remove' AND name = 'x';

DROP TABLE comment_then_remove;
