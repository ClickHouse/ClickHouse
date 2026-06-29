-- Wider-than-64-bit numerics (`Int128/256`, `UInt128/256`,
-- `Decimal64/128/256`) lose precision well before their natural range when
-- routed through `getFloat64`, and `ClusterMergingTransform` only has an
-- exact arithmetic path for narrow ints, `UInt64`/`Int64`, `DateTime64`,
-- and `Time64`. Reject the unsupported numeric key types upfront with
-- a clear `BAD_ARGUMENTS` instead of silently misclustering.

SELECT count() FROM VALUES('x Int128', (toInt128(1)))
GROUP BY x WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM VALUES('x UInt128', (toUInt128(1)))
GROUP BY x WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM VALUES('x Int256', (toInt256(1)))
GROUP BY x WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM VALUES('x UInt256', (toUInt256(1)))
GROUP BY x WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM VALUES('x Decimal64(2)', (toDecimal64(1, 2)))
GROUP BY x WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM VALUES('x Decimal128(4)', (toDecimal128(1, 4)))
GROUP BY x WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

-- 2D mixed: numeric scalar + wide numeric is still wide.
SELECT count() FROM VALUES('x UInt64, y Int128', (1, toInt128(2)))
GROUP BY (x, y) WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
