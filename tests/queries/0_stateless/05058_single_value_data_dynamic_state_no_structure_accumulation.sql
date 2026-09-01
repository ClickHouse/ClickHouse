-- A state over Dynamic or JSON holds a single row, so churning it over rows with different variants
-- or different JSON paths must serialize exactly like the state of the winning row alone.

SELECT
    (SELECT hex(argMaxState(d, n)) FROM (SELECT number AS n, multiIf(number % 3 = 0, number::Dynamic, number % 3 = 1, toString(number)::Dynamic, toDate(number)::Dynamic) AS d FROM numbers(60))) =
    (SELECT hex(argMaxState(d, n)) FROM (SELECT 59::UInt64 AS n, toDate(59)::Dynamic AS d));

SELECT
    (SELECT hex(argMaxState(j, n)) FROM (SELECT number AS n, ('{"p' || toString(number) || '": ' || toString(number) || '}')::JSON AS j FROM numbers(60))) =
    (SELECT hex(argMaxState(j, n)) FROM (SELECT 59::UInt64 AS n, '{"p59": 59}'::JSON AS j));

SELECT
    (SELECT hex(anyLastState(j)) FROM (SELECT ('{"p' || toString(number) || '": ' || toString(number) || '}')::JSON AS j FROM numbers(60)) SETTINGS max_threads = 1, max_block_size = 60) =
    (SELECT hex(anyLastState(j)) FROM (SELECT '{"p59": 59}'::JSON AS j));

-- The finalized values stay correct per group after the same churn.
SELECT k, argMax(j, n) FROM (
    SELECT number % 4 AS k, number AS n, ('{"p' || toString(number) || '": ' || toString(number) || '}')::JSON AS j FROM numbers(60)
) GROUP BY k ORDER BY k;

-- Keeping the JSON paths the state has already seen would make each of the 600 one-row states carry
-- all 500 paths of its group; it took more than 500 MiB before, and stays under 30 MiB now.
SELECT count(s) FROM
(
    SELECT number % 600 AS k, anyLastState(j) AS s
    FROM (SELECT number, ('{"p' || toString(intDiv(number, 600) % 500) || '": 1}')::JSON AS j FROM numbers(300000))
    GROUP BY k
)
SETTINGS max_threads = 1, max_block_size = 500, max_bytes_before_external_group_by = 0, max_memory_usage = '150Mi';
