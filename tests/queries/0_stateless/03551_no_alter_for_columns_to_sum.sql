DROP TABLE IF EXISTS t0;
SET allow_suspicious_primary_key = 1;
CREATE TABLE t0 (c0 Int) ENGINE = SummingMergeTree((c0)) ORDER BY tuple();
ALTER TABLE t0 RENAME COLUMN c0 TO c1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t0;
