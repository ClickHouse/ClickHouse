-- Query result previews (the `query_result_previews` setting) are delivered only to consumers that
-- tell them apart from the result (the native protocol of the initiator). An internal consumer of an
-- asynchronously pulled pipeline, such as the evaluation of a scalar subquery, which takes the first
-- non-empty block, must never see a preview in place of the final result.

SET query_result_previews = 1;
SET query_result_previews_min_interval_ms = 0;
SET query_result_previews_min_rows = 1;
SET max_threads = 4;
SET max_block_size = 65536;

SELECT (SELECT sum(number) FROM numbers(10000000));
SELECT (SELECT count() FROM numbers(10000000) WHERE number % 3 = 0);
SELECT (SELECT max(k) FROM (SELECT number % 1000 AS k, count() FROM numbers(10000000) GROUP BY k));
SELECT 1 WHERE (SELECT count() FROM numbers(10000000)) = 10000000;
