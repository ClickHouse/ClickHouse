-- Tags: no-old-analyzer

-- `max_rows_in_join` / `max_bytes_in_join` limit the build side of a block nested loop join whether
-- it stays in memory or spills to a temporary file, so `join_overflow_mode` means the same either
-- way: spilling relieves memory pressure, it is not a way around the limit.

-- No algorithm is enabled, so every join below is answered by the operator, `INNER` included.
SET join_algorithm = '';
SET query_plan_join_swap_table = 'false';

SELECT 'throw';
SELECT count() FROM numbers(5) l JOIN numbers(10) r ON l.number < r.number
SETTINGS max_rows_in_join = 3, join_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT count() FROM numbers(5) l JOIN numbers(10) r ON l.number < r.number
SETTINGS max_rows_in_join = 3, join_overflow_mode = 'throw', max_bytes_before_external_join = 1; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT count() FROM numbers(5) l LEFT JOIN numbers(10) r ON l.number < r.number
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- `break` keeps the block that reached the limit and stops reading the build side there, so with
-- one build stream and blocks of two rows the join sees exactly the first four build rows. The
-- build side is squashed to `min_joined_block_size_rows` before it is stored, which is what decides
-- how coarse that granularity is, so the two-row blocks only survive with the squashing turned off.
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;

SELECT 'break';
SELECT arraySort(groupArray((l.number, r.number))) FROM numbers(5) l JOIN numbers(10) r ON l.number < r.number
SETTINGS max_rows_in_join = 3, join_overflow_mode = 'break', max_block_size = 2, max_threads = 1;

SELECT 'break spilled',
    (SELECT arraySort(groupArray((l.number, r.number))) FROM numbers(5) l JOIN numbers(10) r ON l.number < r.number
     SETTINGS max_rows_in_join = 3, join_overflow_mode = 'break', max_block_size = 2, max_threads = 1,
              max_bytes_before_external_join = 1)
  = (SELECT arraySort(groupArray((l.number, r.number))) FROM numbers(5) l JOIN numbers(10) r ON l.number < r.number
     SETTINGS max_rows_in_join = 3, join_overflow_mode = 'break', max_block_size = 2, max_threads = 1) AS ok;

-- Squashed to one block, the build side is read whole before the limit is looked at, exactly as it
-- is for a hash join, whose right pipeline is squashed the same way.
SELECT 'break squashed', arraySort(groupArray((l.number, r.number))) FROM numbers(5) l JOIN numbers(10) r ON l.number < r.number
SETTINGS max_rows_in_join = 3, join_overflow_mode = 'break', max_block_size = 2, max_threads = 1,
         min_joined_block_size_rows = 65409;

-- A limit the build side never reaches leaves the result whole.
SELECT 'no overflow', count() FROM numbers(5) l JOIN numbers(10) r ON l.number < r.number
SETTINGS max_rows_in_join = 1000, join_overflow_mode = 'throw';
