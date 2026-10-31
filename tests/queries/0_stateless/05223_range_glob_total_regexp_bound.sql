-- A `{N..M}` range glob becomes a regexp alternation of every number of the range. Capping one
-- range does not cap their sum: every range below is within the per-range limit, yet a path of a
-- few hundred bytes asks for tens of megabytes of alternations.
SELECT * FROM file(repeat('{1..100000}', 128)); -- { serverError BAD_ARGUMENTS }

-- A single range is still expanded, including one whose only value is the largest `size_t`.
SELECT count() FROM file('{18446744073709551615..18446744073709551615}.csv', CSV, 'x UInt8');
