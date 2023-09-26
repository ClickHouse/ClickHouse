-- Checks that duplicate primary keys and skipping indices cannot be created by CREATE or ALTER TABLE unless allow_suspicious_indices = 1.

-- CREATE TABLE

DROP TABLE IF EXISTS tbl;

CREATE TABLE tbl (id UInt32) ENGINE = MergeTree() ORDER BY (id + 1, id + 1);  -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_indices = 1;
CREATE TABLE tbl (id UInt32) ENGINE = MergeTree() ORDER BY (id + 1, id + 1);
SET allow_suspicious_indices = default;

DROP TABLE tbl;
CREATE TABLE tbl (id UInt32) ENGINE = MergeTree() ORDER BY (id + 1, id + 1) SETTINGS index_granularity = 8192 SETTINGS allow_suspicious_indices = 1; -- unbeautiful alternative syntax

DROP TABLE tbl;
CREATE TABLE tbl (id UInt32, INDEX idx (id + 1, id, id + 1) TYPE minmax) ENGINE = MergeTree() ORDER BY id;  -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_indices = 1;
CREATE TABLE tbl (id UInt32, INDEX idx (id + 1, id, id + 1) TYPE minmax) ENGINE = MergeTree() ORDER BY id;
SET allow_suspicious_indices = default;

DROP TABLE tbl;
CREATE TABLE tbl (id UInt32, INDEX idx (id + 1, id, id + 1) TYPE minmax) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8192 SETTINGS allow_suspicious_indices = 1;

-- ALTER TABLE

DROP TABLE tbl;
CREATE TABLE tbl (id1 UInt32) ENGINE = MergeTree() ORDER BY id1;

ALTER TABLE tbl ADD COLUMN `id2` UInt32, MODIFY ORDER BY (id2, id1, id2);  -- { serverError BAD_ARGUMENTS }
ALTER TABLE tbl ADD COLUMN `id2` UInt32, MODIFY ORDER BY (id1, id2, id1);  -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_indices = 1;
ALTER TABLE tbl ADD COLUMN `id2` UInt32, MODIFY ORDER BY (id1, id2, id2);
SET allow_suspicious_indices = default;

ALTER TABLE tbl ADD COLUMN `id3` UInt32, MODIFY ORDER BY (id1, id2, id3, id3) SETTINGS allow_suspicious_indices = 1;

DROP TABLE tbl;
CREATE TABLE tbl (id UInt32) ENGINE = MergeTree() ORDER BY id;

ALTER TABLE tbl ADD INDEX idx (id+1, id, id+1) TYPE minmax;  -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_indices = 1;
ALTER TABLE tbl ADD INDEX idx (id + 1, id, id + 1) TYPE minmax SETTINGS allow_suspicious_indices = 1;
SET allow_suspicious_indices = default;

ALTER TABLE tbl ADD INDEX idx2 (id + 1, id, id + 1) TYPE minmax SETTINGS allow_suspicious_indices = 1;

DROP TABLE tbl;
