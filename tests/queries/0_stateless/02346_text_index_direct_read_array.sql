-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel -- due to access to the system.text_log
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

SET log_queries = 1;

-- Affects the number of read rows.
SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET max_rows_to_read = 0; -- system.text_log can be really big
SET enable_analyzer = 0; -- To produce consistent explain outputs

----------------------------------------------------
SELECT '- Test direct read optimization from text log';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    arr Array(String),
    arr_fixed Array(FixedString(3)),
    INDEX array_idx(arr) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX array_fixed_idx(arr_fixed) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
)
ENGINE = MergeTree()
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab SELECT 2 * number, ['foo', 'bar'], ['foo', 'bar'] FROM numbers(64);
INSERT INTO tab SELECT 2 * number + 1, ['baz def'], ['baz'] FROM numbers(64);

SELECT 'has support';

SELECT '-- with String';
SELECT count() FROM tab WHERE has(arr, 'foo');
SELECT count() FROM tab WHERE has(arr, 'bar');
SELECT count() FROM tab WHERE has(arr, 'baz');
SELECT count() FROM tab WHERE has(arr, 'def');

SELECT '-- with FixedString';
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('foo', 3));
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('bar', 3));
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('baz', 3));

----------------------------------------------------
-- Now check the logs all at once (one by one is too slow)
----------------------------------------------------
SYSTEM FLUSH LOGS text_log;

SELECT message
FROM (
     SELECT event_time_microseconds, message FROM system.text_log
     WHERE logger_name = 'optimizeDirectReadFromTextIndex' AND startsWith(message, 'Added:')
     ORDER BY event_time_microseconds DESC LIMIT 7
)
ORDER BY event_time_microseconds ASC;

----------------------------------------------------
-- Now check that EXPLAIN produces the expected output for the same queries.
-- So this AFTER checking the text_log otherwise the entries will be duplicated.
----------------------------------------------------
SELECT '- Test direct read optimization with EXPLAIN';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE has(arr, 'foo') SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column:%';


SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('foo', 3)) SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column:%';


SELECT '- Test some corner cases (just in case)';
-- This at the end, after we checked the logs
SELECT count() FROM tab WHERE has(arr, 'foo') AND has(arr, 'bar'); -- all foo contain bar
SELECT count() FROM tab WHERE has(arr, 'foo') OR has(arr, 'bar'); -- only the foo contain bar

SELECT count() FROM tab WHERE has(arr, 'baz') AND has(arr, 'def'); -- all baz contain def, but throw tokenizer
SELECT count() FROM tab WHERE has(arr, 'baz') OR has(arr, 'def'); -- only baz contain def, but throw tokenizer

SELECT count() FROM tab WHERE has(arr, 'foo') AND has(arr, 'def'); -- no foo contain def


DROP TABLE tab;
