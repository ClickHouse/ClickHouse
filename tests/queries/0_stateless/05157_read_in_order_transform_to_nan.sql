-- The `ORDER BY` expression maps a stored `-inf` to `NaN`, which sorts after everything else, so the
-- forward read of the key does not produce the requested order: the read-in-order optimization must
-- not treat such a transform as monotonic. No `NaN` is in the data or in the query.

SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_read_in_order_nan;

CREATE TABLE t_read_in_order_nan (x Float64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_read_in_order_nan VALUES (-inf), (1), (2), (3);

SELECT x / inf AS n FROM t_read_in_order_nan ORDER BY n;
SELECT x / inf AS n FROM t_read_in_order_nan ORDER BY n LIMIT 2;
SELECT x / inf AS n FROM t_read_in_order_nan ORDER BY n SETTINGS optimize_read_in_order = 0;
SELECT x * 0 AS n FROM t_read_in_order_nan ORDER BY n;
SELECT x * nan AS n FROM t_read_in_order_nan ORDER BY n;

-- An ordinary constant cannot produce a `NaN`, so the order still comes from the key.

SELECT x / 2 AS n FROM t_read_in_order_nan ORDER BY n;
SELECT x * 2 AS n FROM t_read_in_order_nan ORDER BY n LIMIT 2;

-- The read type: 1 = in order, 0 = the order is not taken from the key.
SELECT 'Float64 key';
SELECT 'x / inf', countIf(explain LIKE '%InOrder%') FROM (EXPLAIN PLAN actions = 1 SELECT x / inf AS n FROM t_read_in_order_nan ORDER BY n);
SELECT 'x * 0.', countIf(explain LIKE '%InOrder%') FROM (EXPLAIN PLAN actions = 1 SELECT x * 0. AS n FROM t_read_in_order_nan ORDER BY n);
SELECT 'x / 0', countIf(explain LIKE '%InOrder%') FROM (EXPLAIN PLAN actions = 1 SELECT x / 0 AS n FROM t_read_in_order_nan ORDER BY n);
SELECT 'x * inf', countIf(explain LIKE '%InOrder%') FROM (EXPLAIN PLAN actions = 1 SELECT x * inf AS n FROM t_read_in_order_nan ORDER BY n);
SELECT 'x * nan', countIf(explain LIKE '%InOrder%') FROM (EXPLAIN PLAN actions = 1 SELECT x * nan AS n FROM t_read_in_order_nan ORDER BY n);
SELECT 'x / 2', countIf(explain LIKE '%InOrder%') FROM (EXPLAIN PLAN actions = 1 SELECT x / 2 AS n FROM t_read_in_order_nan ORDER BY n);

DROP TABLE t_read_in_order_nan;

-- An integer key cannot hold `±inf`, so `x / inf` cannot produce a `NaN` from it and the key is still
-- read in order; `x / 0` produces a `NaN` at `x = 0`, which any key can hold. (`x * c` with unknown
-- endpoints is declined for every constant, independently of this check, so only `divide` is shown.)

CREATE TABLE t_read_in_order_nan_int (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_read_in_order_nan_int VALUES (0), (1), (2), (3);

SELECT 'UInt64 key';
SELECT 'x / inf', countIf(explain LIKE '%InOrder%') FROM (EXPLAIN PLAN actions = 1 SELECT x / inf AS n FROM t_read_in_order_nan_int ORDER BY n);
SELECT 'x / 0', countIf(explain LIKE '%InOrder%') FROM (EXPLAIN PLAN actions = 1 SELECT x / 0 AS n FROM t_read_in_order_nan_int ORDER BY n);
SELECT x / inf AS n FROM t_read_in_order_nan_int ORDER BY n;
SELECT x / 0 AS n FROM t_read_in_order_nan_int ORDER BY n;
SELECT x * inf AS n FROM t_read_in_order_nan_int ORDER BY n;

-- Index analysis over the key must not prune the matching rows either way.
SELECT count() FROM t_read_in_order_nan_int WHERE x / inf = 0;
SELECT count() FROM t_read_in_order_nan_int WHERE x * 0. = 0;
SELECT count() FROM t_read_in_order_nan_int WHERE x / 0 = inf;

DROP TABLE t_read_in_order_nan_int;
