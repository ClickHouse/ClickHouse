-- Tags: no-old-analyzer, no-random-settings

-- Pairs waiting to be emitted keep alive the build block they came from, and a compressed or spilled
-- block is materialized anew on every read. A condition selective enough to match a few rows in every
-- block used to accumulate one such block per stored block, so the whole build side came back into
-- memory and both compressing and spilling it were undone. The output chunk is now cut before that
-- happens, which is what these memory caps assert: the build side is ~30 MB decompressed, an order of
-- magnitude more than they allow.

SET enable_analyzer = 1;
SET cross_to_inner_join_rewrite = 0;
SET max_threads = 1;
SET max_block_size = 4096;

DROP TABLE IF EXISTS bnl_ret_build;
CREATE TABLE bnl_ret_build (y UInt64, t String) ENGINE = MergeTree ORDER BY y;
INSERT INTO bnl_ret_build SELECT number, repeat(toString(number % 10), 500) FROM numbers(60000);

SELECT 'spilled', count(), sum(cityHash64(r.t))
FROM (SELECT number AS x FROM numbers(1)) l
LEFT JOIN bnl_ret_build r ON r.y > l.x AND (r.y % 4096) < l.x + 1
SETTINGS max_bytes_before_external_join = '1Mi', max_memory_usage = '20Mi';

SELECT 'compressed', count(), sum(cityHash64(r.t))
FROM (SELECT number AS x FROM numbers(1)) l
LEFT JOIN bnl_ret_build r ON r.y > l.x AND (r.y % 4096) < l.x + 1
SETTINGS cross_join_min_rows_to_compress = 1, max_memory_usage = '20Mi';

DROP TABLE bnl_ret_build;
