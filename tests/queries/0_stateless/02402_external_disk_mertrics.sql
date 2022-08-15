-- Tags: no-parallel, no-fasttest, long

SET max_bytes_before_external_sort = 33554432;
set max_block_size = 1048576;

SELECT number FROM (SELECT number FROM numbers(2097152)) ORDER BY number * 1234567890123456789 LIMIT 2097142, 10
SETTINGS log_comment='02402_external_disk_mertrics/sort'
FORMAT Null;

SET max_bytes_before_external_group_by = '100M';
SET max_memory_usage = '410M';
SET group_by_two_level_threshold = '100K';
SET group_by_two_level_threshold_bytes = '50M';

SELECT sum(k), sum(c) FROM (SELECT number AS k, count() AS c FROM (SELECT * FROM system.numbers LIMIT 2097152) GROUP BY k)
SETTINGS log_comment='02402_external_disk_mertrics/aggregation'
FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT
    any(ProfileEvents['ExternalSortWritePart']) >= 1 AND
    any(ProfileEvents['ExternalSortMerge']) >= 1 AND
    any(ProfileEvents['ExternalSortCompressedBytes']) >= 100000 AND
    any(ProfileEvents['ExternalSortUncompressedBytes']) >= 100000 AND
    count() == 1
    FROM system.query_log WHERE current_database = currentDatabase()
        AND log_comment = '02402_external_disk_mertrics/sort'
        AND query ILIKE 'SELECT%2097152%' AND type = 'QueryFinish';

SELECT
    any(ProfileEvents['ExternalAggregationWritePart']) >= 1 AND
    any(ProfileEvents['ExternalAggregationMerge']) >= 1 AND
    any(ProfileEvents['ExternalAggregationCompressedBytes']) >= 100000 AND
    any(ProfileEvents['ExternalAggregationUncompressedBytes']) >= 100000 AND
    count() == 1
    FROM system.query_log WHERE current_database = currentDatabase()
        AND log_comment = '02402_external_disk_mertrics/aggregation'
        AND query ILIKE 'SELECT%2097152%' AND type = 'QueryFinish';
