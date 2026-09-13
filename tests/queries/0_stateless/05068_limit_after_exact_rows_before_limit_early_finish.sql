-- With `exact_rows_before_limit`, `rows_before_limit_at_least` counts every row before the range even when
-- a downstream processor finishes early, exactly as for a plain `LIMIT`: the range transform keeps
-- reading and counting after its output is closed. `extremes` makes the output format wait for the
-- extremes port, which closes only once the whole stream below the range is exhausted, so the reported
-- count does not depend on the scheduling of the drain.
SET exact_rows_before_limit = 1;
SET extremes = 1;
SET output_format_write_statistics = 0;
SET max_block_size = 1;
SET max_result_rows = 1;
SET result_overflow_mode = 'break';

SELECT number FROM numbers(20) ORDER BY number LIMIT 5 AFTER number >= 5 FORMAT JSONCompact SETTINGS enable_analyzer = 1;
SELECT number FROM numbers(20) ORDER BY number LIMIT 5 AFTER number >= 5 FORMAT JSONCompact SETTINGS enable_analyzer = 0;
SELECT number FROM numbers(20) LIMIT 5 AFTER number >= 5 FORMAT JSONCompact SETTINGS enable_analyzer = 1;
SELECT number FROM numbers(20) LIMIT 5 AFTER number >= 5 FORMAT JSONCompact SETTINGS enable_analyzer = 0;
