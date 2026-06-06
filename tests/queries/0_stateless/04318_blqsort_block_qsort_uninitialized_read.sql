-- Regression test for an uninitialized read of `right_offs` in blqsort's `block_qsort`
-- (contrib/blqsort/blqs.h). `groupArraySorted` over a generic column sorts a vector of
-- `Field` with `::sort`, which dispatches to `block_qsort` because `Field` is larger than
-- 16 bytes. For this 21-element input the median-of-medians pivot is the maximum of the
-- range, so the partitioning loop never fills the right block, leaving `right_offs`
-- unassigned; the post-loop cleanup then read it. Under MemorySanitizer this aborted with
-- use-of-uninitialized-value.
--
-- The argument is greater than 1000000 to select the sort strategy, which keeps the values
-- in insertion order so the sort sees exactly this sequence. `max_threads = 1` keeps the
-- aggregation order deterministic. The values are zero-padded so the lexicographic order of
-- the strings matches the integer order of the original reproducer.

SELECT groupArraySorted(2000000)(s)
FROM values('s String', '13','20','09','05','06','02','13','02','01','16','01','10','07','15','06','07','09','03','08','06','19')
SETTINGS max_threads = 1;
