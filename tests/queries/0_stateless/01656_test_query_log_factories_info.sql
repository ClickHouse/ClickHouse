SELECT uniqArray([1, 1, 2]),
       SUBSTRING('Hello, world', 7, 5),
       CAST(arrayJoin([NULL, NULL]) AS Nullable(UInt8)),
       avgOrDefaultIf(number, number % 2),
       sumOrNull(number),
       toTypeName(sumOrNull(number)),
       countIf(toDate('2000-12-05') + number as d,
       toDayOfYear(d) % 2)
FROM numbers(100);
SELECT '';

SYSTEM FLUSH LOGS;
SELECT arraySort(used_aggregate_functions)
FROM system.query_log WHERE type = 'QueryFinish' AND (query LIKE '%toDate(\'2000-12-05\')%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

SELECT arraySort(used_table_functions)
FROM system.query_log WHERE type = 'QueryFinish' AND (query LIKE '%toDate(\'2000-12-05\')%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

SELECT arraySort(used_functions)
FROM system.query_log WHERE type = 'QueryFinish' AND (query LIKE '%toDate(\'2000-12-05\')%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

SELECT arraySort(used_data_types)
FROM system.query_log WHERE type = 'QueryFinish' AND (query LIKE '%toDate(\'2000-12-05\')%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

DROP database IF EXISTS test_query_log_factories_info1;
CREATE database test_query_log_factories_info1 ENGINE=Atomic;

SYSTEM FLUSH LOGS;
SELECT used_databases
FROM system.query_log
WHERE type == 'QueryFinish' AND (query LIKE '%database test_query_log_factories_info%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

CREATE OR REPLACE TABLE test_query_log_factories_info1.memory_table (id UInt64, date DateTime) ENGINE=Memory();

SYSTEM FLUSH LOGS;
SELECT arraySort(used_data_types), used_storages
FROM system.query_log
WHERE type == 'QueryFinish' AND (query LIKE '%TABLE test%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

