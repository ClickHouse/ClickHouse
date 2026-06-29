-- Verify that DateTime64 primary key columns benefit from key condition analysis
-- when compared against integer constants (epoch values).
-- Previously, only DateTime (not DateTime64) was recognized as cast-free comparable
-- with native integers, causing key analysis to produce "Condition: true" (no pruning).

DROP TABLE IF EXISTS t_datetime64_key;

CREATE TABLE t_datetime64_key (dt DateTime64(3)) ENGINE = MergeTree() ORDER BY dt SETTINGS index_granularity = 1;

INSERT INTO t_datetime64_key VALUES ('2020-01-01 00:00:00.000'), ('2020-01-02 00:00:00.000'), ('2020-01-03 00:00:00.000');

-- Compare DateTime64 key with integer constant (epoch ms beyond data range).
-- With proper key analysis all granules are pruned and 0 rows are read.
-- Without the fix, key condition is "true", all rows are scanned, exceeding max_rows_to_read.
SELECT count() FROM t_datetime64_key WHERE dt > 1735689600000 SETTINGS max_rows_to_read = 1;

-- Sanity check: DateTime (not DateTime64) already works
DROP TABLE IF EXISTS t_datetime_key;
CREATE TABLE t_datetime_key (dt DateTime) ENGINE = MergeTree() ORDER BY dt SETTINGS index_granularity = 1;
INSERT INTO t_datetime_key VALUES ('2020-01-01 00:00:00'), ('2020-01-02 00:00:00'), ('2020-01-03 00:00:00');
SELECT count() FROM t_datetime_key WHERE dt > 1735689600 SETTINGS max_rows_to_read = 1;

DROP TABLE t_datetime64_key;
DROP TABLE t_datetime_key;
