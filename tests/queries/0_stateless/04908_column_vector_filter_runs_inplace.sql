-- Use a `MergeTree` `PREWHERE` to exercise the in-place `ColumnVector` filter overload.
SET max_block_size = 256;
SET max_threads = 1;

DROP TABLE IF EXISTS t_04908;
CREATE TABLE t_04908 (k UInt64, payload UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_04908 SELECT number, number FROM numbers(65536);

SELECT groupArray(payload) = arrayFilter(
    x -> (x % 64 BETWEEN 4 AND 11)
      OR (x % 64 BETWEEN 20 AND 35)
      OR (x % 64 BETWEEN 48 AND 55),
    range(65536))
FROM t_04908
PREWHERE (k % 64 BETWEEN 4 AND 11)
      OR (k % 64 BETWEEN 20 AND 35)
      OR (k % 64 BETWEEN 48 AND 55);

DROP TABLE t_04908;
