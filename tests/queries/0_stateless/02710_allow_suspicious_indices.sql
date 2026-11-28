-- Check CREATE TABLE

DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (id UInt32) ENGINE = MergeTree() ORDER BY (id + 1, id + 1);  -- { serverError BAD_ARGUMENTS }
CREATE TABLE tbl (id UInt32) ENGINE = MergeTree() ORDER BY (id + 1, id + 1) SETTINGS allow_suspicious_indices = 1;

DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (id UInt32, INDEX idx (id + 1, id + 1) TYPE minmax) ENGINE = MergeTree() ORDER BY id;  -- { serverError BAD_ARGUMENTS }
CREATE TABLE tbl (id UInt32, INDEX idx (id + 1, id + 1) TYPE minmax) ENGINE = MergeTree() ORDER BY id SETTINGS allow_suspicious_indices = 1;

-- Check ALTER TABLE

DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (id1 UInt32) ENGINE = MergeTree() ORDER BY id1;
ALTER TABLE tbl ADD COLUMN `id2` UInt32, MODIFY ORDER BY (id1, id2, id2);  -- { serverError BAD_ARGUMENTS }
ALTER TABLE tbl ADD COLUMN `id2` UInt32, MODIFY ORDER BY (id1, id2, id1);  -- { serverError BAD_ARGUMENTS }
ALTER TABLE tbl ADD COLUMN `id2` UInt32, MODIFY ORDER BY (id1, id2, id2) SETTINGS allow_suspicious_indices = 1;

DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (id UInt32) ENGINE = MergeTree() ORDER BY id;
ALTER TABLE tbl ADD INDEX idx (id+1, id, id+1) TYPE minmax;  -- { serverError BAD_ARGUMENTS }
ALTER TABLE tbl ADD INDEX idx (id+1, id, id+1) TYPE minmax SETTINGS allow_suspicious_indices = 1;
