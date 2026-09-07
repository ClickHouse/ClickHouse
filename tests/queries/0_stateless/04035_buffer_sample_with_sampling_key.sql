-- Positive-path coverage for PR #99306 (Buffer SAMPLE delegation to destination table).
-- StorageBuffer::supportsSampling was changed to delegate to the destination table,
-- so SAMPLE must succeed when the destination MergeTree has SAMPLE BY.

DROP TABLE IF EXISTS t_04035_mt;
DROP TABLE IF EXISTS t_04035_buf;

CREATE TABLE t_04035_mt (x UInt64, y String) ENGINE = MergeTree ORDER BY x SAMPLE BY x;
CREATE TABLE t_04035_buf (x UInt64, y String)
    ENGINE = Buffer(currentDatabase(), t_04035_mt, 1, 0, 0, 1, 1, 1, 1);

INSERT INTO t_04035_mt SELECT number, toString(number) FROM numbers(10000);

SELECT count() > 0 FROM t_04035_buf SAMPLE 0.5;
SELECT count() > 0 FROM t_04035_buf SAMPLE 0.5 SETTINGS enable_analyzer = 0;

DROP TABLE t_04035_buf;
DROP TABLE t_04035_mt;
