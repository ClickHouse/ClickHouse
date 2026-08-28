-- Among values that compare equal in the sort order but differ in the binary representation, the
-- post-spill DISTINCT must keep the first-received one, which requires the spilled chunks to be sorted
-- with a stable sort. The million unique fillers guarantee that the spill happens long before the
-- zeros start; the zeros then interleave the two bit patterns starting with -0., so exactly one zero
-- must come out and it must keep the -0. bit pattern of the first-received row (an unstable sort could
-- move any of the interleaved 0. rows to the front of their sort-equal range). The assertions aggregate
-- over the DISTINCT result instead of filtering it, so that no predicate is pushed down below the
-- DISTINCT (a pushed-down filter would drop the fillers and let the zeros reach the in-memory phase).
SELECT count(), countIf(k = 0), sumIf(reinterpretAsUInt64(k), k = 0)
FROM (SELECT DISTINCT k FROM (SELECT if(number < 1000000, (number + 10000000)::Float64, if(number % 2 = 0, -0., 0.)) AS k FROM numbers(1100000)))
SETTINGS max_bytes_before_external_distinct = 1, max_bytes_ratio_before_external_distinct = 0, max_untracked_memory = 0, max_threads = 1;
