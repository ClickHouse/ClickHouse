-- Tags: no-old-analyzer, no-random-settings

-- Pairs waiting to be emitted keep alive the build block they came from, and a compressed or spilled
-- block is materialized anew on every read. A condition selective enough to match a few rows in every
-- block used to accumulate one such block per stored block, so the whole build side came back into
-- memory and both compressing and spilling it were undone. The output chunk is now cut before that
-- happens, which is what these memory caps assert: the build side is ~30 MB decompressed, an order of
-- magnitude more than they allow. The caps are also what asserts that the spill and the compression
-- happened at all - a build side that stayed whole in memory does not fit under them, so the query
-- fails rather than passing vacuously. What the spill accounts for in `ExternalJoin*` is checked in
-- `04821_block_nested_loop_join_spill`, and is not repeated here.

SET enable_analyzer = 1;
SET cross_to_inner_join_rewrite = 0;
-- The condition below is a pair of inequalities, which `ie_join` claims wherever it is enabled - and
-- it is in the default list. The operator under test is the block nested loop join, so leave it out.
SET join_algorithm = 'hash';
SET max_threads = 1;
SET max_block_size = 4096;

-- The build side is generated rather than read from a table: a storage read has memory costs of its
-- own that vary with the disk backend (on s3 disks the read buffers alone broke these caps), and the
-- caps are about the join.
DROP TABLE IF EXISTS bnl_ret_build;
CREATE VIEW bnl_ret_build AS SELECT number AS y, repeat(toString(number % 10), 500) AS t FROM numbers(60000);

SELECT 'spilled', count(), sum(cityHash64(r.t))
FROM (SELECT number AS x FROM numbers(1)) l
LEFT JOIN bnl_ret_build r ON r.y > l.x AND (r.y % 4096) < l.x + 1
SETTINGS max_bytes_before_external_join = '1Mi', max_memory_usage = '20Mi';

SELECT 'compressed', count(), sum(cityHash64(r.t))
FROM (SELECT number AS x FROM numbers(1)) l
LEFT JOIN bnl_ret_build r ON r.y > l.x AND (r.y % 4096) < l.x + 1
SETTINGS cross_join_min_rows_to_compress = 1, max_memory_usage = '20Mi';

-- Each probe stream materializes the blocks it walks for itself, so what the probe phase holds must
-- also not grow with the number of them: what a stream may keep alive is its share of an allowance for
-- the step, and the store keeps its blocks small enough for one of them to be that share. Eight
-- streams each holding the whole build side would need an order of magnitude more than these caps
-- allow. `max_block_size = 1` on the probe source is what gives every stream chunks of its own to
-- walk the store with; it is scoped to the subquery so the walk and the output keep their normal
-- granularity - per-row chunks everywhere would make the query per-row-slow under the flaky check's
-- `ThreadFuzzer`, whose injections tax every extra chunk.
SELECT 'spilled, 8 streams', count(), sum(cityHash64(r.t))
FROM (SELECT number AS x FROM numbers(16) SETTINGS max_block_size = 1) l
LEFT JOIN bnl_ret_build r ON r.y > l.x AND (r.y % 4096) < l.x + 1
SETTINGS max_bytes_before_external_join = '1Mi', max_threads = 8, max_memory_usage = '64Mi';

SELECT 'compressed, 8 streams', count(), sum(cityHash64(r.t))
FROM (SELECT number AS x FROM numbers(16) SETTINGS max_block_size = 1) l
LEFT JOIN bnl_ret_build r ON r.y > l.x AND (r.y % 4096) < l.x + 1
SETTINGS cross_join_min_rows_to_compress = 1, max_threads = 8, max_memory_usage = '40Mi';

DROP TABLE bnl_ret_build;
