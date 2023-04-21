DROP TABLE IF EXISTS allow_suspicious_indices;
CREATE TABLE allow_suspicious_indices (id UInt32) ENGINE = MergeTree() order by (id, id) settings allow_suspicious_indices = 1;
DROP TABLE IF EXISTS allow_suspicious_indices;
CREATE TABLE allow_suspicious_indices (id UInt32) ENGINE = MergeTree() order by (id, id) settings allow_suspicious_indices = 0;  -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS allow_suspicious_indices;
CREATE TABLE allow_suspicious_indices (id UInt32, index idx (id, id) TYPE minmax) ENGINE = MergeTree() order by id settings allow_suspicious_indices = 1;
DROP TABLE IF EXISTS allow_suspicious_indices;
CREATE TABLE allow_suspicious_indices (id UInt32, index idx (id, id) TYPE minmax) ENGINE = MergeTree() order by id settings allow_suspicious_indices = 0;  -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS allow_suspicious_indices;
CREATE TABLE allow_suspicious_indices (id UInt32) ENGINE = MergeTree() order by id + 1 settings allow_suspicious_indices = 0;
ALTER TABLE allow_suspicious_indices ADD COLUMN `id2` UInt32, MODIFY ORDER BY (id + 1, id2 + 1, id2 + 1);  -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS allow_suspicious_indices;
CREATE TABLE allow_suspicious_indices (id UInt32) ENGINE = MergeTree() order by id + 1 settings allow_suspicious_indices = 0;
ALTER TABLE allow_suspicious_indices ADD COLUMN `id2` UInt32, MODIFY ORDER BY (id + 1, id2 + 1, id + 1);  -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS allow_suspicious_indices;
CREATE TABLE allow_suspicious_indices (id UInt32) ENGINE = MergeTree() order by id + 1 settings allow_suspicious_indices = 0;
ALTER TABLE allow_suspicious_indices ADD INDEX idx (id+1, id+1) type minmax;  -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS allow_suspicious_indices;
CREATE TABLE allow_suspicious_indices (id UInt32) ENGINE = MergeTree() order by id + 1 settings allow_suspicious_indices = 1;
ALTER TABLE allow_suspicious_indices ADD INDEX idx (id+1, id+1) type minmax;
DROP TABLE IF EXISTS allow_suspicious_indices;

SET allow_suspicious_indices = 1;
CREATE TABLE allow_suspicious_indices (id UInt32) ENGINE = MergeTree() order by (id);
ALTER TABLE allow_suspicious_indices ADD COLUMN `id2` UInt32, MODIFY ORDER BY (id, id2 + 1, id2 + 1);
DROP TABLE IF EXISTS allow_suspicious_indices;
CREATE TABLE allow_suspicious_indices (id UInt32) ENGINE = MergeTree() order by (id);
ALTER TABLE allow_suspicious_indices ADD INDEX idx (id+1, id+1) type minmax;
