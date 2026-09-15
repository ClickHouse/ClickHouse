-- Rows re-read from the temporary files of an external sort must not be counted again as
-- source reads. Every run below reads exactly 100000 rows from `numbers`, so `read_rows` and
-- `SelectedRows` must be 100000 on every path. `currentDatabase` scopes the server-global
-- `query_log`, so concurrent copies of this test cannot read each other's rows.
--
-- `getReadProgress` reports rows and bytes together, so the byte counters are asserted as well.
-- They are not implied by the row counters: a source row is 8 bytes (`number`), while a re-read
-- row carries the sort expression too and is 16, so before the fix the rows inflated 2x while
-- the bytes inflated 3x (2400000 against the correct 800000).
--
-- Both spilling sources are covered, and each cell asserts which of them actually emitted, so
-- a cell cannot silently stop covering its source: at `max_bytes_before_external_sort` = 100000
-- everything goes through `BufferingFromFileSource`, at 1000000 one part spills and the rest is
-- re-served by `MergeSorterSource`.
--
-- `max_block_size` is pinned because it decides that split, and the test runner draws it from
-- 8000..100000; at the top of that range `MergeSorterSource` emits nothing.
-- `ast_fuzzer_runs` is pinned because the stress profile enables the server-side AST fuzzer for
-- any query, and a fuzzed re-execution inherits `log_comment` and would win the `argMax` below.
-- `log_processors_profiles` is pinned because randomized `compatibility` below 24.3 reverts it
-- to its old `false` default, which would empty the per-source join.

-- in-memory control: a fix that deflates every path must still look wrong here
SELECT number FROM numbers(100000) ORDER BY sipHash64(number)
SETTINGS log_queries = 1, log_comment = '04653_memory',
    log_processors_profiles = 1, ast_fuzzer_runs = 0,
    max_threads = 1, max_memory_usage = 134217728, max_block_size = 65409,
    max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0
FORMAT Null;

-- spills 2 parts, all re-read rows come from `BufferingFromFileSource`
SELECT number FROM numbers(100000) ORDER BY sipHash64(number)
SETTINGS log_queries = 1, log_comment = '04653_spill_buffering',
    log_processors_profiles = 1, ast_fuzzer_runs = 0,
    max_threads = 1, max_memory_usage = 134217728, max_block_size = 65409,
    max_bytes_before_external_sort = 100000, max_bytes_ratio_before_external_sort = 0
FORMAT Null;

-- spills 1 part, the rest stays in memory and is re-read by `MergeSorterSource`
SELECT number FROM numbers(100000) ORDER BY sipHash64(number)
SETTINGS log_queries = 1, log_comment = '04653_spill_mergesorter',
    log_processors_profiles = 1, ast_fuzzer_runs = 0,
    max_threads = 1, max_memory_usage = 134217728, max_block_size = 65409,
    max_bytes_before_external_sort = 1000000, max_bytes_ratio_before_external_sort = 0
FORMAT Null;

-- top-K never spills; must-not-regress row
SELECT number FROM numbers(100000) ORDER BY sipHash64(number) LIMIT 10
SETTINGS log_queries = 1, log_comment = '04653_limit',
    log_processors_profiles = 1, ast_fuzzer_runs = 0,
    max_threads = 1, max_memory_usage = 134217728, max_block_size = 65409,
    max_bytes_before_external_sort = 100000, max_bytes_ratio_before_external_sort = 0
FORMAT Null;

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- `log_comment` is matched exactly rather than by prefix: the test runner injects a
-- client-level comment that also starts with 04653, and it would otherwise be measured too.
SELECT
    q.cell AS cell,
    if(q.read_rows = 100000 AND q.selected_rows = 100000
        AND q.read_bytes = 800000 AND q.selected_bytes = 800000
        AND q.write_parts = if(cell IN ('04653_memory', '04653_limit'), 0, if(cell = '04653_spill_buffering', 2, 1))
        AND (p.buffering_rows > 0) = (cell IN ('04653_spill_buffering', '04653_spill_mergesorter'))
        AND (p.mergesorter_rows > 0) = (cell = '04653_spill_mergesorter'),
        'ok',
        'fail: read_rows=' || toString(q.read_rows)
            || ' SelectedRows=' || toString(q.selected_rows)
            || ' read_bytes=' || toString(q.read_bytes)
            || ' SelectedBytes=' || toString(q.selected_bytes)
            || ' ExternalSortWritePart=' || toString(q.write_parts)
            || ' BufferingFromFileSource=' || toString(p.buffering_rows)
            || ' MergeSorterSource=' || toString(p.mergesorter_rows)) AS result
FROM
(
    SELECT log_comment AS cell,
        argMax(query_id, event_time_microseconds) AS qid,
        argMax(read_rows, event_time_microseconds) AS read_rows,
        argMax(read_bytes, event_time_microseconds) AS read_bytes,
        argMax(ProfileEvents['SelectedRows'], event_time_microseconds) AS selected_rows,
        argMax(ProfileEvents['SelectedBytes'], event_time_microseconds) AS selected_bytes,
        argMax(ProfileEvents['ExternalSortWritePart'], event_time_microseconds) AS write_parts
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND log_comment IN ('04653_memory', '04653_spill_buffering', '04653_spill_mergesorter', '04653_limit')
    GROUP BY cell
) AS q
LEFT JOIN
(
    SELECT query_id AS qid,
        sumIf(output_rows, name = 'BufferingFromFileSource') AS buffering_rows,
        sumIf(output_rows, name = 'MergeSorterSource') AS mergesorter_rows
    FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
    GROUP BY qid
) AS p USING (qid)
ORDER BY cell;
