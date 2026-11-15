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

-- A dependent may read another dependent, so the recomputation is closed transitively and ordered:
-- `m2` has to see the recomputed `m1`, not the one computed before the expiry.
DROP TABLE IF EXISTS t_column_ttl_materialized_chain;
CREATE TABLE t_column_ttl_materialized_chain
(
    d DateTime,
    x Int32 TTL d + INTERVAL 1 SECOND,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 + 1,
    m3 Int32 MATERIALIZED m2 + 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_column_ttl_materialized_chain (d, x) VALUES ('2000-01-01 00:00:00', 41);
OPTIMIZE TABLE t_column_ttl_materialized_chain FINAL;
SELECT x, m1, m2, m3 FROM t_column_ttl_materialized_chain;

DROP TABLE t_column_ttl_materialized_chain;

-- A dependent that has a TTL of its own is recomputed as well: only the rows whose own TTL is due keep
-- the default, and the ones it is not due for get the value their expression states.
DROP TABLE IF EXISTS t_column_ttl_materialized_own_ttl;
CREATE TABLE t_column_ttl_materialized_own_ttl
(
    d DateTime,
    e DateTime,
    x Int32 TTL d + INTERVAL 1 SECOND,
    m Int32 MATERIALIZED x + 1 TTL e + INTERVAL 1 SECOND
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_column_ttl_materialized_own_ttl (d, e, x) VALUES ('2000-01-01 00:00:00', '2100-01-01 00:00:00', 41);
OPTIMIZE TABLE t_column_ttl_materialized_own_ttl FINAL;
SELECT x, m FROM t_column_ttl_materialized_own_ttl;

DROP TABLE t_column_ttl_materialized_own_ttl;

-- A dependent that reads a subcolumn of the expired column, and one that reads it through an `ALIAS`.
DROP TABLE IF EXISTS t_column_ttl_materialized_indirect;
CREATE TABLE t_column_ttl_materialized_indirect
(
    d DateTime,
    x Int32 TTL d + INTERVAL 1 SECOND,
    t Tuple(a Int32, b Int32) TTL d + INTERVAL 1 SECOND,
    a Int32 ALIAS x + 100,
    m_alias Int32 MATERIALIZED a + 1,
    m_subcolumn Int32 MATERIALIZED t.a + 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_column_ttl_materialized_indirect (d, x, t) VALUES ('2000-01-01 00:00:00', 41, (7, 8));
OPTIMIZE TABLE t_column_ttl_materialized_indirect FINAL;
SELECT x, t, m_alias, m_subcolumn FROM t_column_ttl_materialized_indirect;

DROP TABLE t_column_ttl_materialized_indirect;

-- A `GROUP BY` TTL emits the last group it accumulated at the end of the part, after the last block was
-- consumed, and the column TTL resets its column in that block too, so the dependent has to be
-- recomputed there as well. `k = 2` is the group that comes out at the end.
DROP TABLE IF EXISTS t_column_ttl_materialized_group_by;
CREATE TABLE t_column_ttl_materialized_group_by
(
    k Int32,
    d DateTime,
    e DateTime,
    x Int32 TTL e + INTERVAL 1 SECOND,
    m Int32 MATERIALIZED x + 1
)
ENGINE = MergeTree ORDER BY k
TTL d + INTERVAL 1 DAY GROUP BY k SET e = max(e)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_column_ttl_materialized_group_by (k, d, e, x) VALUES
    (1, '2000-01-01 00:00:00', '2100-01-01 00:00:00', 41),
    (2, '2000-01-01 00:00:00', '2000-01-01 00:00:00', 42);
OPTIMIZE TABLE t_column_ttl_materialized_group_by FINAL;
SELECT k, x, m FROM t_column_ttl_materialized_group_by ORDER BY k;

DROP TABLE t_column_ttl_materialized_group_by;

-- A `MATERIALIZED` expression that is not defined on the default of the column it reads has no correct
-- value after the expiry, and the error is not hidden: the merge fails instead of writing a value that
-- contradicts the expression.
DROP TABLE IF EXISTS t_column_ttl_materialized_undefined;
CREATE TABLE t_column_ttl_materialized_undefined
(
    d DateTime,
    x Int32 TTL d + INTERVAL 1 SECOND,
    m Int32 MATERIALIZED intDiv(100, x)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_column_ttl_materialized_undefined (d, x) VALUES ('2000-01-01 00:00:00', 41);
OPTIMIZE TABLE t_column_ttl_materialized_undefined FINAL; -- { serverError ILLEGAL_DIVISION }

DROP TABLE t_column_ttl_materialized_undefined;
