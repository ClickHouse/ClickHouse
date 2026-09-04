-- Wider-than-64-bit numerics (`Int128/256`, `UInt128/256`) lose precision well
-- before their natural range when routed through `getFloat64`, and
-- `ClusterMergingTransform` only has an exact arithmetic path for narrow ints,
-- `UInt64`/`Int64`, `DateTime64`, and `Time64`. Every `Decimal` type is rejected
-- too (including `Decimal32`): the distance check runs in `Float64`, so exact
-- decimal boundaries depend on binary rounding. Reject the unsupported numeric
-- key types upfront with a clear `BAD_ARGUMENTS` instead of silently misclustering.

SET allow_experimental_group_by_with_cluster = 1;

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

-- `Decimal32` is rejected too: an exact decimal boundary such as `0.7` and `0.8`
-- with `WITH CLUSTER 0.1` (decimal distance exactly the threshold) would be split
-- by the `Float64` comparison `0.7 + 0.1 >= 0.8` (`0.7999999999999999 < 0.8`).
SELECT count() FROM VALUES('x Decimal32(1)', (toDecimal32(0.7, 1)), (toDecimal32(0.8, 1)))
GROUP BY x WITH CLUSTER 0.1; -- { serverError BAD_ARGUMENTS }

-- 2D mixed: numeric scalar + wide numeric is still wide.
SELECT count() FROM VALUES('x UInt64, y Int128', (1, toInt128(2)))
GROUP BY (x, y) WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
