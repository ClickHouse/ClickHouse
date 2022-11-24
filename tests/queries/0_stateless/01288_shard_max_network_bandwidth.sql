-- Tags: shard

-- Limit to 10 MB/sec
SET max_network_bandwidth = 10000000;

-- Lower max_block_size, so we can start throttling sooner. Otherwise query will be executed too quickly.
SET max_block_size = 100;

CREATE TEMPORARY TABLE times (t DateTime);

-- rand64 is uncompressable data. Each number will take 8 bytes of bandwidth.
-- This query should execute in no less than 1.6 seconds if throttled.
INSERT INTO times SELECT now();
SELECT sum(ignore(*)) FROM (SELECT rand64() FROM remote('127.0.0.{2,3}', numbers(2000000)));
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
