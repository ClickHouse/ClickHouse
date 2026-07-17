-- Tags: no-parallel, memory-engine

SET database_atomic_wait_for_drop_and_detach_synchronously=1;
SET log_queries=1;

SELECT uniqArray([1, 1, 2]),
       SUBSTRING('Hello, world', 7, 5),
       POW(1, 2), ROUND(TANh(1)), CrC32(''),
       SUM(number), MAX(number),
       flatten([[[BIT_AND(123)]], [[mod(3, 2)], [CAST('1' AS INTEGER)]]]),
       week(toDate('2000-12-05')),
       CAST(arrayJoin([NULL, NULL]) AS Nullable(TEXT)),
       avgOrDefaultIf(number, number % 2),
       sumOrNull(number),
       toTypeName(sumOrNull(number)),
       countIf(toDate('2000-12-05') + number as d,
       toDayOfYear(d) % 2)
FROM numbers(100);

SELECT repeat('aa', number)
FROM numbers(10e3)
SETTINGS max_memory_usage=4e6, max_block_size=100
FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT '';

SYSTEM FLUSH LOGS query_log;

SELECT arraySort(used_aggregate_functions)
FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND (query LIKE '%toDate(\'2000-12-05\')%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

SELECT arraySort(used_aggregate_function_combinators)
FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND (query LIKE '%toDate(\'2000-12-05\')%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

SELECT arraySort(used_table_functions)
FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND (query LIKE '%toDate(\'2000-12-05\')%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

-- 1. analyzer includes arrayJoin into functions list
-- 2. for crc32 (CaseInsensitive function) we use lower case now
SELECT arraySort(arrayMap(x -> x == 'crc32' ? 'CRC32' : x, arrayFilter(x-> x != 'arrayJoin', used_functions))) as `arraySort(used_functions)`
FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND (query LIKE '%toDate(\'2000-12-05\')%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

SELECT used_functions
FROM system.query_log WHERE current_database = currentDatabase() AND type != 'QueryStart' AND (query LIKE '%repeat%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

SELECT arraySort(used_data_type_families)
FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND (query LIKE '%toDate(\'2000-12-05\')%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

DROP database IF EXISTS test_query_log_factories_info1;
CREATE database test_query_log_factories_info1 ENGINE=Atomic;

SYSTEM FLUSH LOGS query_log;
SELECT used_database_engines
FROM system.query_log
WHERE current_database = currentDatabase() AND type == 'QueryFinish' AND (query LIKE '%database test_query_log_factories_info%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

CREATE OR REPLACE TABLE test_query_log_factories_info1.memory_table (id BIGINT, date DATETIME, date2 DateTime) ENGINE=Memory();

SYSTEM FLUSH LOGS query_log;
SELECT arraySort(used_data_type_families), used_storages
FROM system.query_log
WHERE current_database = currentDatabase() AND type == 'QueryFinish' AND (query LIKE '%TABLE test%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

DROP TABLE test_query_log_factories_info1.memory_table;
DROP DATABASE test_query_log_factories_info1;
