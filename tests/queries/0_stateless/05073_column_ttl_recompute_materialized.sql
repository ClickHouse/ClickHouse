-- A column TTL resets its column to the default, so the `MATERIALIZED` columns computed from it have to
-- be recomputed - otherwise they keep the pre-expiry value forever, contradicting their own expression.

DROP TABLE IF EXISTS t_column_ttl_materialized;
CREATE TABLE t_column_ttl_materialized
(
    d DateTime,
    x Int32 TTL d + INTERVAL 1 SECOND,
    y Int32,
    mx Int32 MATERIALIZED x + 1,
    my Int32 MATERIALIZED y * 10
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_column_ttl_materialized (d, x, y) VALUES ('2000-01-01 00:00:00', 41, 5);
INSERT INTO t_column_ttl_materialized (d, x, y) VALUES ('2100-01-01 00:00:00', 7, 6);
OPTIMIZE TABLE t_column_ttl_materialized FINAL;

-- The expired row's `mx` follows the reset value; the row that did not expire and the `MATERIALIZED`
-- column that does not read `x` are untouched.
SELECT x, mx, y, my FROM t_column_ttl_materialized ORDER BY d;

DROP TABLE t_column_ttl_materialized;

-- A `MATERIALIZED` column that takes part in the sorting key is not recomputed: that would invalidate
-- the order of the part being written.
DROP TABLE IF EXISTS t_column_ttl_materialized_key;
CREATE TABLE t_column_ttl_materialized_key (d DateTime, x Int32 TTL d + INTERVAL 1 SECOND, mx Int32 MATERIALIZED x + 1)
ENGINE = MergeTree ORDER BY mx SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_column_ttl_materialized_key (d, x) VALUES ('2000-01-01 00:00:00', 41);
INSERT INTO t_column_ttl_materialized_key (d, x) VALUES ('2000-01-02 00:00:00', 42);
OPTIMIZE TABLE t_column_ttl_materialized_key FINAL;
SELECT x, mx FROM t_column_ttl_materialized_key ORDER BY mx;

DROP TABLE t_column_ttl_materialized_key;
