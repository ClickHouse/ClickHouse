-- Tags: no-parallel, no-fasttest, no-flaky-check

SET max_bytes_ratio_before_external_sort = 0;

-- { echoOn }
SELECT number FROM (SELECT number FROM system.numbers LIMIT 10000000) ORDER BY number * 1234567890123456789 LIMIT 9999990, 10 SETTINGS max_memory_usage='300Mi', max_bytes_before_external_sort='20M', min_external_sort_block_bytes='70M';
SELECT number FROM (SELECT number FROM system.numbers LIMIT 10000000) ORDER BY number * 1234567890123456789 LIMIT 9999990, 10 SETTINGS max_memory_usage='300Mi', max_bytes_before_external_sort='10M', min_external_sort_block_bytes='10M';
SELECT number FROM (SELECT number FROM numbers(2097152)) ORDER BY number * 1234567890123456789 LIMIT 2097142, 10 SETTINGS max_memory_usage='300Mi', max_bytes_before_external_sort=33554432, max_block_size=1048576, min_external_sort_block_bytes='10Mi';

-- This query is heavy, let's do it only once
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['ExternalSortWritePart'] FROM system.query_log WHERE type != 'QueryStart' AND current_database = currentDatabase() AND Settings['max_bytes_before_external_sort']='20000000';
SELECT if((ProfileEvents['ExternalSortWritePart'] as x) > 10, 10, x) FROM system.query_log WHERE type != 'QueryStart' AND current_database = currentDatabase() AND Settings['max_bytes_before_external_sort']='10000000';
SELECT ProfileEvents['ExternalSortWritePart'] FROM system.query_log WHERE type != 'QueryStart' AND current_database = currentDatabase() AND Settings['max_bytes_before_external_sort']='33554432';
