SELECT uniq(1, 1, 2, 2), count(toDate('2000-12-05') + number as d) FROM numbers(100);
SYSTEM FLUSH LOGS;
SELECT used_aggregate_functions, used_table_functions, used_functions
FROM system.query_log WHERE type = 'QueryFinish' AND (query LIKE '%toDate(\'2000-12-05\')%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

SELECT CAST(arrayJoin([NULL, NULL]) AS Nullable(UInt8)) AS x;
SYSTEM FLUSH LOGS;
SELECT used_functions, used_data_types
FROM system.query_log WHERE type = 'QueryFinish' AND query LIKE 'SELECT CAST(arrayJ%'
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

DROP database IF EXISTS test_query_log_factories_info1;
CREATE database test_query_log_factories_info1 ENGINE=Atomic;
CREATE OR REPLACE TABLE test_query_log_factories_info1.memory_table (event String, date DateTime) ENGINE=Memory();
SYSTEM FLUSH LOGS;
SELECT used_databases
FROM system.query_log
WHERE type == 'QueryFinish' AND (query LIKE '%database test_query_log_factories_info%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT used_data_types, used_storages
FROM system.query_log
WHERE type == 'QueryFinish' AND (query LIKE '%TABLE test%')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;

