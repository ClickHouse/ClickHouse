-- The `ORDER BY` expression maps a stored `-inf` to `NaN`, which sorts after everything else, so the
-- forward read of the key does not produce the requested order: the read-in-order optimization must
-- not treat such a transform as monotonic. No `NaN` is in the data or in the query.

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

DROP TABLE t_read_in_order_nan;
