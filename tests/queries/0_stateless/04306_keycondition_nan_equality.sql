DROP TABLE IF EXISTS t_keycond_nan_eq;

CREATE TABLE t_keycond_nan_eq (c0 Int32) ENGINE = MergeTree
ORDER BY sqrt(c0)
SETTINGS allow_suspicious_indices = 1, index_granularity = 4;

INSERT INTO t_keycond_nan_eq SELECT number - 50 FROM numbers(100);

-- Equality predicate on c0 must return the row even though sqrt(c0) is NaN for negative c0.
-- Previously `c0 = -30` was translated into `sqrt(c0) in [nan, nan]` and pruned all granules
-- of negative values, which also poisoned the query condition cache.

SELECT count() FROM t_keycond_nan_eq WHERE c0 = -30;
SELECT count() FROM t_keycond_nan_eq WHERE c0 = -1;
SELECT count() FROM t_keycond_nan_eq WHERE c0 = -50;
SELECT count() FROM t_keycond_nan_eq WHERE c0 = 0;
SELECT count() FROM t_keycond_nan_eq WHERE c0 = 49;

-- Control queries that were already correct.
SELECT countIf(c0 = -30) FROM t_keycond_nan_eq;
SELECT count() FROM t_keycond_nan_eq WHERE c0 IN (-30);
SELECT count() FROM t_keycond_nan_eq WHERE c0 < -20;
SELECT count() FROM t_keycond_nan_eq WHERE c0 = -30::Int32;

-- Repeated queries after the index path ran must still be correct
-- (catches query condition cache poisoning).
SELECT count() FROM t_keycond_nan_eq WHERE c0 = -30 SETTINGS use_primary_key = 0;
SELECT count() FROM t_keycond_nan_eq WHERE c0 = -1 SETTINGS use_primary_key = 0;

-- Same shape with `log` (also undefined for non-positive c0).
DROP TABLE IF EXISTS t_keycond_nan_eq_log;
CREATE TABLE t_keycond_nan_eq_log (c0 Int32) ENGINE = MergeTree
ORDER BY log(c0)
SETTINGS allow_suspicious_indices = 1, index_granularity = 4;
INSERT INTO t_keycond_nan_eq_log SELECT number - 50 FROM numbers(100);

SELECT count() FROM t_keycond_nan_eq_log WHERE c0 = -30;
SELECT count() FROM t_keycond_nan_eq_log WHERE c0 = 0;
SELECT count() FROM t_keycond_nan_eq_log WHERE c0 = 5;

DROP TABLE t_keycond_nan_eq_log;
DROP TABLE t_keycond_nan_eq;
