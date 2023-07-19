-- Tags: no-s3-storage
-- We allocate a lot of memory for buffers when reading or writing to S3

DROP TABLE IF EXISTS 02725_memory_for_merges SYNC;

CREATE TABLE 02725_memory_for_merges
(   n UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY n
SETTINGS merge_max_block_size_bytes=1024, index_granularity_bytes=1024;

INSERT INTO 02725_memory_for_merges SELECT number, randomPrintableASCII(1000000) FROM numbers(100);
INSERT INTO 02725_memory_for_merges SELECT number, randomPrintableASCII(1000000) FROM numbers(100);
INSERT INTO 02725_memory_for_merges SELECT number, randomPrintableASCII(1000000) FROM numbers(100);
INSERT INTO 02725_memory_for_merges SELECT number, randomPrintableASCII(1000000) FROM numbers(100);
INSERT INTO 02725_memory_for_merges SELECT number, randomPrintableASCII(1000000) FROM numbers(100);

OPTIMIZE TABLE 02725_memory_for_merges FINAL;

SYSTEM FLUSH LOGS;

WITH (SELECT uuid FROM system.tables WHERE table='02725_memory_for_merges' and database=currentDatabase()) as uuid
SELECT sum(peak_memory_usage) < 1024 * 1024 * 200 from system.part_log where table_uuid=uuid and event_type='MergeParts';

DROP TABLE IF EXISTS 02725_memory_for_merges SYNC;
