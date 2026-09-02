SET max_block_size = 1000;
SET max_threads = 10;
SELECT
    groupUniqArray(blockSize()),
    uniqExact(rowNumberInAllBlocks()),
    min(rowNumberInAllBlocks()),
    max(rowNumberInAllBlocks()),
    uniqExact(rowNumberInBlock()),
    min(rowNumberInBlock()),
    max(rowNumberInBlock()),
    uniqExact(blockNumber())
FROM (SELECT * FROM system.numbers_mt LIMIT 100000);
