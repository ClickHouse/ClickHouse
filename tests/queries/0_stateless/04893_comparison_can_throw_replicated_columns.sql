-- Comparisons over lazily replicated columns.
-- Whether the rows that the query does not reference are removed from a replicated column before a
-- function is executed depends on `canThrow` of that function, and for comparisons it depends on the
-- argument types: comparing decimals cannot throw, whatever their scales, comparing `DateTime64` of different
-- scales can.
-- The results must be the same either way.

SET enable_lazy_columns_replication = 1;

DROP TABLE IF EXISTS t_cmp_probe;
DROP TABLE IF EXISTS t_cmp_build;

CREATE TABLE t_cmp_probe
(
    k UInt32,
    dec20 Decimal128(20),
    dec30 Decimal128(30),
    s String,
    d Date,
    dt64_3 DateTime64(3, 'UTC'),
    dt64_6 DateTime64(6, 'UTC'),
    u UUID
)
ENGINE = MergeTree ORDER BY k;

CREATE TABLE t_cmp_build (k UInt32) ENGINE = MergeTree ORDER BY k;

-- Only `k = 1` is joined, so the rows with `k = 2` stay in the replicated columns while
-- being referenced by nothing. Their values are the ones that overflow when rescaled.
INSERT INTO t_cmp_probe VALUES (1, 1, 1, 'a', '2020-01-01', '2020-01-01 00:00:00.001', '2020-01-01 00:00:00.001', '00000000-0000-0000-0000-000000000001');
INSERT INTO t_cmp_probe VALUES (2, 1000000000, 1, 'b', '2021-01-01', '2021-01-01 00:00:00.002', '2021-01-01 00:00:00.002', '00000000-0000-0000-0000-000000000002');

INSERT INTO t_cmp_build VALUES (1);

-- Comparisons that cannot throw: same types, equal scales.
SELECT dec20 = dec20, s = 'a', s = 'b', d = d, dt64_3 = dt64_3, u = u
FROM t_cmp_probe JOIN t_cmp_build USING (k)
ORDER BY ALL;

-- Different decimal scales, compared exactly when the rescale overflows, and comparisons that can throw:
-- different `DateTime64` scales, a date compared to a string.
SELECT dec20 = dec30, dec20 = toDecimal128(2, 30), dt64_3 = dt64_6, d = '2020-01-01', d = '2021-01-01'
FROM t_cmp_probe JOIN t_cmp_build USING (k)
ORDER BY ALL;

-- The same through a filter, so that the comparison is executed on a replicated column
-- whose unreferenced rows were left behind by the filter.
SELECT dec20 = dec30, dec20 = toDecimal128(2, 30), dt64_3 = dt64_6
FROM (SELECT * FROM t_cmp_probe JOIN t_cmp_build USING (k) WHERE s = 'a')
ORDER BY ALL;

SELECT isNotDistinctFrom(dec20, dec20), isNotDistinctFrom(dec20, dec30)
FROM t_cmp_probe JOIN t_cmp_build USING (k)
ORDER BY ALL;

-- A `Decimal128` compared with a `UInt128` can throw: on the unreferenced row, the `UInt128` does not fit into `Int128`.
-- The row with `k = 1` is joined twice, so that its 16-byte values are replicated lazily. The join is pinned so that
-- both rows reach it on the probe side.
DROP TABLE IF EXISTS t_cmp_probe_u128;
CREATE TABLE t_cmp_probe_u128 (k UInt32, dec Decimal128(2), u UInt128) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_cmp_probe_u128 VALUES (1, 1, 1), (2, 1, 200000000000000000000000000000000000000);

SELECT dec = u, dec < u
FROM t_cmp_probe_u128 JOIN (SELECT toUInt32(1) AS k FROM numbers(2)) AS b USING (k)
ORDER BY ALL
SETTINGS decimal_check_overflow = 1, join_algorithm = 'hash', enable_join_runtime_filters = 0, query_plan_join_swap_table = 0,
    query_plan_optimize_join_order_limit = 0;

-- Both rows joined and replicated twice each, so the comparisons run on repeated values.
INSERT INTO t_cmp_build VALUES (1), (2), (2);

SELECT k, dec20 = dec30, dt64_3 = dt64_6, s = 'a', d = '2020-01-01'
FROM t_cmp_probe JOIN t_cmp_build USING (k)
ORDER BY ALL;

DROP TABLE t_cmp_probe;
DROP TABLE t_cmp_build;
DROP TABLE t_cmp_probe_u128;
