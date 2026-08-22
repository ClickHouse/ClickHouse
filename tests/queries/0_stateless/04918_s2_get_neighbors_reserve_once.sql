-- Tags: no-fasttest
-- Tag no-fasttest: needs s2

-- The result accumulator is per block, so max_block_size is pinned here: clickhouse-test
-- randomizes it and an unpinned block size makes the memory limit below meaningless.
-- Reserving the accumulator once stays under 120 MB at two million rows; reserving it per row
-- needs more than 190 MB, because each reserve charges the new size before releasing the old one.
SELECT sum(length(s2GetNeighbors(geoToS2(number * 0.0000001, 40.0))))
FROM numbers(2000000)
SETTINGS max_block_size = 2000000, max_threads = 1, max_memory_usage = 160000000;

SELECT s2GetNeighbors(5765131099823669248);
SELECT length(s2GetNeighbors(materialize(5765131099823669248)));
SELECT sum(length(s2GetNeighbors(geoToS2(number * 0.0000001, 40.0)))) FROM numbers(1000);

-- Many small blocks: the reserved size differs most from the per-row shape here.
SELECT sum(cityHash64(arrayJoin(s2GetNeighbors(geoToS2(number * 0.0000001, 40.0)))))
FROM numbers(20000) SETTINGS max_block_size = 7;

SELECT count(), sum(length(s2GetNeighbors(geoToS2(number * 0.0000001, 40.0)))) FROM numbers(0);
