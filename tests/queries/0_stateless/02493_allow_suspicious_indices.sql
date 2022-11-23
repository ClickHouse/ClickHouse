DROP DATABASE IF EXISTS test;
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE test.allow_suspicious_indices (id UInt32) ENGINE = MergeTree() order by (id, id) settings allow_suspicious_indices = 1;
DROP TABLE IF EXISTS test.allow_suspicious_indices;
CREATE TABLE test.allow_suspicous_indices (id UInt32) ENGINE = MergeTree() order by (id, id) settings allow_suspicious_indices = 0;  -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS test.allow_suspicious_indices;
CREATE TABLE test.allow_suspicous_indices (id UInt32, index idx (id, id) TYPE minmax GRANULARITY 1) ENGINE = MergeTree() order by id settings allow_suspicious_indices = 1;
DROP TABLE IF EXISTS test.allow_suspicious_indices;
CREATE TABLE test.allow_suspicious_indices (id UInt32, index idx (id, id) TYPE minmax GRANULARITY 1) ENGINE = MergeTree() order by id settings allow_suspicious_indices = 0;  -- { serverError BAD_ARGUMENTS }
