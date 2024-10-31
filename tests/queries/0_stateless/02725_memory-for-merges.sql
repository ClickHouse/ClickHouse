-- Tags: no-object-storage, no-random-merge-tree-settings
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

SELECT (sum(peak_memory_usage) < 1024 * 1024 * 200 AS x) ? x : sum(peak_memory_usage) from system.part_log where database=currentDatabase() and table='02725_memory_for_merges' and event_type='MergeParts';

DROP TABLE IF EXISTS 02725_memory_for_merges SYNC;
