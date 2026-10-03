SELECT * FROM (SELECT range(number) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT arrayMap(x -> toNullable(x), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT arrayMap(x -> (x, x), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT arrayMap(x -> (x, x + 1), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT arrayMap(x -> (x, toNullable(x)), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT arrayMap(x -> (x, nullIf(x, 3)), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;

-- A filter whose kept rows are longer than average (here the dropped rows are empty, as with `s != ''`) must reserve the
-- kept data at once: growing the row-proportional estimate holds the old and the new buffer.
DROP TABLE IF EXISTS t_filter_reserve;
CREATE TABLE t_filter_reserve (n UInt64, s String, a Array(Nullable(UInt8))) ENGINE = Memory;
-- One block; every 8th row holds all the data: 1000 bytes in `s`, 1000 elements in `a`.
INSERT INTO t_filter_reserve
SELECT number, if(number % 8 = 0, repeat('x', 1000), ''), if(number % 8 = 0, arrayResize([1::Nullable(UInt8)], 1000), [])
FROM numbers(80000) SETTINGS max_block_size = 80000;

SELECT s FROM t_filter_reserve WHERE n % 8 = 0 SETTINGS log_comment = 'long s' FORMAT Null;
SELECT a FROM t_filter_reserve WHERE n % 8 = 0 SETTINGS log_comment = 'long a' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Kept data: 10 MB and 20 MB (elements and null map). The row-proportional estimate peaks at 25 and 42 MB.
SELECT log_comment, memory_usage < if(log_comment = 'long s', 12000000, 24000000)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('long s', 'long a')
ORDER BY log_comment;

DROP TABLE t_filter_reserve;
