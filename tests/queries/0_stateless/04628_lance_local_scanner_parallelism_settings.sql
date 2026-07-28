-- Tags: no-parallel
-- Ordered vs unordered scanner settings must preserve aggregates (row set).
-- ProfileEvents asserted via system.query_log.

DROP TABLE IF EXISTS lance_local_scanner_parallelism;

CREATE TABLE lance_local_scanner_parallelism
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

SELECT count(), sum(id), sum(cityHash64(name))
FROM lance_local_scanner_parallelism
SETTINGS lance_scan_in_order = 1;

SELECT count(), sum(id), sum(cityHash64(name))
FROM lance_local_scanner_parallelism
SETTINGS
    lance_scan_in_order = 0,
    lance_fragment_readahead = 4,
    lance_batch_readahead = 4;

SET log_queries = 1;

SELECT count()
FROM lance_local_scanner_parallelism
FORMAT Null
SETTINGS
    lance_scan_in_order = 0,
    lance_fragment_readahead = 4,
    log_comment = 'lance_scanner_unordered';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['LanceScanUnordered'] > 0
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'lance_scanner_unordered'
ORDER BY event_time_microseconds DESC
LIMIT 1
SETTINGS enable_parallel_replicas = 0;

DROP TABLE lance_local_scanner_parallelism;
