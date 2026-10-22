-- Tags: no-replicated-database

DROP TABLE IF EXISTS src_table;
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS mv_to;
DROP TABLE IF EXISTS target_table;

CREATE TABLE src_table (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv (id UInt64 COMMENT 'i') ENGINE = MergeTree ORDER BY id AS SELECT id FROM src_table;

-- MODIFY QUERY replaces the view's columns, so a comment command after it in the same statement names a
-- column the statement itself removed. That is a user error, not an internal one.
ALTER TABLE mv (MODIFY QUERY SELECT id + 1 AS id2 FROM src_table), (COMMENT COLUMN id 'c'); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
ALTER TABLE mv (MODIFY QUERY SELECT id + 1 AS id2 FROM src_table), (MODIFY COLUMN id COMMENT 'c'); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

-- With IF EXISTS the comment is skipped, and the statement fails on the inner table instead.
ALTER TABLE mv (MODIFY QUERY SELECT id + 1 AS id2 FROM src_table), (COMMENT COLUMN IF EXISTS id 'c'); -- { serverError NO_SUCH_COLUMN_IN_TABLE }

SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'mv' ORDER BY name;

-- Control: the guard must not fire when the new query keeps the column, so this statement succeeds.
-- What the view stores afterwards is the inner table's column set, so the comment value is not asserted here.
ALTER TABLE mv (MODIFY QUERY SELECT id FROM src_table), (COMMENT COLUMN id 'c');

CREATE TABLE target_table (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_to TO target_table AS SELECT id FROM src_table;
ALTER TABLE mv_to (MODIFY QUERY SELECT id + 1 AS id2 FROM src_table), (COMMENT COLUMN id 'c'); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

DROP TABLE mv_to;
DROP TABLE target_table;
DROP TABLE mv;
DROP TABLE src_table;
