-- A Nullable(Bool) column used inside a partition key must filter identically
-- whether referenced implicitly (WHERE c0) or explicitly (WHERE c0 = TRUE):
-- NULL is falsy in both, so the row counts must match. Previously they diverged.
DROP TABLE IF EXISTS t_04401;

CREATE TABLE t_04401 (c0 Nullable(Bool), c1 Nullable(UInt64), c2 Nullable(UInt128))
ENGINE = MergeTree() PARTITION BY (murmurHash3_32(c0, c1, c2)) PRIMARY KEY (c2)
SETTINGS allow_nullable_key = 1;

INSERT INTO t_04401
SELECT multiIf(number % 3 = 0, NULL, number % 3 = 1, true, false), number, number
FROM numbers(60);

-- Ground truth is a full-scan aggregate: countIf has no WHERE, so it cannot use
-- partition pruning. Every WHERE form must equal it, otherwise pruning dropped
-- (or kept) the wrong partitions. Each assertion yields 1.

-- Implicit-bool filter, partition pruning enabled (default).
SELECT (SELECT count() FROM t_04401 WHERE c0)
     = (SELECT countIf(c0 = TRUE) FROM t_04401);
-- Explicit = TRUE, partition pruning enabled (default).
SELECT (SELECT count() FROM t_04401 WHERE c0 = TRUE)
     = (SELECT countIf(c0 = TRUE) FROM t_04401);
-- Implicit-bool filter, partition pruning and skip indexes genuinely disabled.
SELECT (SELECT count() FROM t_04401 WHERE c0 SETTINGS use_partition_pruning = 0, use_skip_indexes = 0)
     = (SELECT countIf(c0 = TRUE) FROM t_04401);
-- Explicit = TRUE, partition pruning and skip indexes genuinely disabled.
SELECT (SELECT count() FROM t_04401 WHERE c0 = TRUE SETTINGS use_partition_pruning = 0, use_skip_indexes = 0)
     = (SELECT countIf(c0 = TRUE) FROM t_04401);

DROP TABLE t_04401;
