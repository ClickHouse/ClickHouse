-- Tags: distributed, long, no-flaky-check
-- ^ no-flaky-check - sometimes longer than 600s with ThreadFuzzer.

SET max_memory_usage = 150000000;
SET max_bytes_before_external_sort = 10000000;
SET max_bytes_ratio_before_external_sort = 0;
SET min_external_sort_block_bytes = '10M';
SET max_threads = 8;

DROP TABLE IF EXISTS numbers10m;
CREATE VIEW numbers10m AS SELECT number FROM system.numbers LIMIT 5000000;

SELECT number FROM numbers10m ORDER BY number * 1234567890123456789 LIMIT 4999980, 20;
SELECT '-';
SELECT number FROM remote('127.0.0.{2,3}', currentDatabase(), numbers10m) ORDER BY number * 1234567890123456789 LIMIT 4999980, 20;

DROP TABLE numbers10m;
