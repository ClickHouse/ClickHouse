-- Tags: long, no-fasttest, no-tsan, no-asan, no-msan
-- A part holding more than 1 GiB of Array(FixedString) elements in one read span must be readable.
SET allow_suspicious_fixed_string_types = 1;
SET max_memory_usage = '8G';

DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt64, arr Array(FixedString(1048576))) ENGINE = MergeTree ORDER BY k;
INSERT INTO t SELECT 1, arrayMap(x -> toFixedString('a', 1048576), range(1025));
SELECT arr[length(arr)] = toFixedString('a', 1048576) FROM t;
DROP TABLE t;
