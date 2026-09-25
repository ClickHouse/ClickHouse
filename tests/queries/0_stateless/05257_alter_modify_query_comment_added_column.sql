-- Tags: no-replicated-database
-- The tag is required: a Replicated database refuses one ALTER that mixes replicated and non replicated
-- commands, and pairing MODIFY QUERY with a comment command is exactly that mix.

-- A comment command written in the same ALTER TABLE statement as a MODIFY QUERY may name a column
-- that the new query introduces. Writing the two commands as two separate ALTER statements always
-- worked; writing them as one statement was rejected because the column did not exist yet.

DROP TABLE IF EXISTS src_table;
DROP TABLE IF EXISTS target_table;
DROP TABLE IF EXISTS mv_comment;
DROP TABLE IF EXISTS mv_modify;
DROP TABLE IF EXISTS mv_comment_if_exists;
DROP TABLE IF EXISTS mv_modify_if_exists;
DROP TABLE IF EXISTS mv_absent;
DROP TABLE IF EXISTS mv_inner;

CREATE TABLE src_table (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE target_table (id UInt64, id2 Nullable(UInt64)) ENGINE = MergeTree ORDER BY id;

CREATE MATERIALIZED VIEW mv_comment TO target_table AS SELECT id FROM src_table;
ALTER TABLE mv_comment (MODIFY QUERY SELECT id, id + 1 AS id2 FROM src_table), (COMMENT COLUMN id2 'c');
SELECT name, comment FROM system.columns WHERE database = currentDatabase() AND table = 'mv_comment' ORDER BY name;

CREATE MATERIALIZED VIEW mv_modify TO target_table AS SELECT id FROM src_table;
ALTER TABLE mv_modify (MODIFY QUERY SELECT id, id + 1 AS id2 FROM src_table), (MODIFY COLUMN id2 COMMENT 'c');
SELECT name, comment FROM system.columns WHERE database = currentDatabase() AND table = 'mv_modify' ORDER BY name;

-- IF EXISTS can only weaken a command, so it must not stop the comment from being applied.
CREATE MATERIALIZED VIEW mv_comment_if_exists TO target_table AS SELECT id FROM src_table;
ALTER TABLE mv_comment_if_exists (MODIFY QUERY SELECT id, id + 1 AS id2 FROM src_table), (COMMENT COLUMN IF EXISTS id2 'c');
SELECT name, comment FROM system.columns WHERE database = currentDatabase() AND table = 'mv_comment_if_exists' ORDER BY name;

CREATE MATERIALIZED VIEW mv_modify_if_exists TO target_table AS SELECT id FROM src_table;
ALTER TABLE mv_modify_if_exists (MODIFY QUERY SELECT id, id + 1 AS id2 FROM src_table), (MODIFY COLUMN IF EXISTS id2 COMMENT 'c');
SELECT name, comment FROM system.columns WHERE database = currentDatabase() AND table = 'mv_modify_if_exists' ORDER BY name;

-- A name that is in neither the old nor the new column set is still rejected, and rejected as a user
-- error rather than an internal one, which is what keeps a typo from becoming an internal error now
-- that the check happens later.
CREATE MATERIALIZED VIEW mv_absent TO target_table AS SELECT id FROM src_table;
ALTER TABLE mv_absent (MODIFY QUERY SELECT id, id + 1 AS id2 FROM src_table), (COMMENT COLUMN absent 'c'); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

-- With IF EXISTS that same name is skipped instead, and the rest of the statement still runs, so the
-- comment on the column the new query does introduce is applied.
ALTER TABLE mv_absent (MODIFY QUERY SELECT id, id + 1 AS id2 FROM src_table), (COMMENT COLUMN IF EXISTS absent 'c'), (COMMENT COLUMN id2 'kept');
SELECT name, comment FROM system.columns WHERE database = currentDatabase() AND table = 'mv_absent' ORDER BY name;

-- A view with an inner table stores the inner table's columns, so the new query introducing a column
-- the inner table does not have is reported against the inner table.
CREATE MATERIALIZED VIEW mv_inner (id UInt64) ENGINE = MergeTree ORDER BY id AS SELECT id FROM src_table;
ALTER TABLE mv_inner (MODIFY QUERY SELECT id, id + 1 AS id2 FROM src_table), (COMMENT COLUMN id2 'c'); -- { serverError NO_SUCH_COLUMN_IN_TABLE }

DROP TABLE mv_inner;
DROP TABLE mv_absent;
DROP TABLE mv_modify_if_exists;
DROP TABLE mv_comment_if_exists;
DROP TABLE mv_modify;
DROP TABLE mv_comment;
DROP TABLE target_table;
DROP TABLE src_table;
