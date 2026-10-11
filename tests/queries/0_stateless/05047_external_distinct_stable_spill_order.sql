-- Values that compare equal in the sort order but differ under hash equality must remain distinct after
-- spilling. The million unique fillers guarantee that spilling starts before the interleaved `-0.0` and
-- `+0.0` values arrive. The assertions aggregate over the `DISTINCT` result instead of filtering it, so no
-- predicate is pushed down below `DISTINCT` and drops the fillers before they force the external path.
SELECT count(), countIf(k = 0), sumIf(reinterpretAsUInt64(k), k = 0)
FROM (SELECT DISTINCT k FROM (SELECT if(number < 1000000, (number + 10000000)::Float64, if(number % 2 = 0, -0., 0.)) AS k FROM numbers(1100000)))
SETTINGS max_bytes_before_external_distinct = 1, max_bytes_ratio_before_external_distinct = 0, max_untracked_memory = 0, max_threads = 1;
