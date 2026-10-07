-- Tags: no-fasttest, no-random-settings, no-asan, no-msan, no-tsan
-- Tag no-fasttest: needs s2
-- Tag no-random-settings: the memory limit below is a fixed budget
-- Tag no-asan, no-msan, no-tsan: fine thresholds on memory usage

-- The result accumulator is per block, so max_block_size is pinned: clickhouse-test
-- randomizes it and an unpinned block size makes the limit below meaningless.
-- Reserving once with the block's exact total stays under the limit; reserving per row
-- does not, because each reserve is exact and therefore charges the new size before
-- releasing the old one. Reserving too little also exceeds it. The limit does not
-- separate reserving once from not reserving at all, because push_back grows the
-- array geometrically on its own.
SELECT sum(length(s2GetNeighbors(geoToS2(number * 0.0000001, 40.0))))
FROM numbers(4000000)
SETTINGS max_block_size = 4000000, max_threads = 1, max_memory_usage = 340000000;

SELECT s2GetNeighbors(5765131099823669248);
SELECT length(s2GetNeighbors(materialize(5765131099823669248)));
SELECT sum(length(s2GetNeighbors(geoToS2(number * 0.0000001, 40.0)))) FROM numbers(1000);

-- Many small blocks: the reserved size differs most from the per-row shape here.
SELECT sum(cityHash64(arrayJoin(s2GetNeighbors(geoToS2(number * 0.0000001, 40.0)))))
FROM numbers(20000) SETTINGS max_block_size = 7;

SELECT count(), sum(length(s2GetNeighbors(geoToS2(number * 0.0000001, 40.0)))) FROM numbers(0);
