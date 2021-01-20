SELECT uniq(1, 1, 2, 2), count(toDate('2000-12-05') + number as d) FROM numbers(100);
INSERT INTO function null('x Int128') SELECT * FROM numbers_mt(100);
SYSTEM FLUSH LOGS;
SELECT factory_aggregate_functions AS aggreagte_functions,
       factory_table_functions AS table_functions,
       factory_functions AS functions
FROM system.query_log WHERE type = 'QueryFinish' AND
(query LIKE '%toDate(\'2000-12-05\')%' OR query LIKE 'null(\'x Int128\')')
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

SELECT CAST(arrayJoin([NULL, NULL]) AS Nullable(UInt8)) AS x;
SYSTEM FLUSH LOGS;
SELECT factory_functions AS functions, factory_data_types as data_types
FROM system.query_log WHERE type = 'QueryFinish' AND query LIKE 'SELECT k, groupArray%'
ORDER BY query_start_time DESC LIMIT 1 FORMAT TabSeparatedWithNames;
SELECT '';

DROP database IF EXISTS test_query_log_factories_info1;
CREATE database test_query_log_factories_info1 ENGINE=Atomic;
CREATE OR REPLACE TABLE test_query_log_factories_info1.memory_table (event String, date DateTime) ENGINE=Memory();
SYSTEM FLUSH LOGS;
SELECT factory_data_types AS data_types, factory_databases AS databases, factory_storages AS storages
FROM system.query_log
WHERE type == 'QueryFinish' AND (query LIKE '%database test_query_log_factories_info%' OR query LIKE '%TABLE test%')
ORDER BY query_start_time DESC LIMIT 2 FORMAT TabSeparatedWithNames;
SELECT '';

