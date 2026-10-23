-- Tags: no-random-merge-tree-settings, no-object-storage
-- no-random-merge-tree-settings: the output enumerates the physical representation, which many randomised settings change
-- no-object-storage: the FileOpen profile event fires only for local-disk reads.

DROP TABLE IF EXISTS t_pushdown;

CREATE TABLE t_pushdown (a UInt64 CODEC(LZ4), b UInt64 CODEC(ZSTD))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_pushdown;

INSERT INTO t_pushdown SELECT number, number FROM numbers(100000);
INSERT INTO t_pushdown SELECT number, number FROM numbers(100000);

SELECT 'all rows';
SELECT part_name, column, substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown)
ORDER BY part_name, column;

SELECT 'where part_name';
SELECT part_name, column FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE part_name = 'all_1_1_0' ORDER BY column;
SELECT 'where column';
SELECT part_name, column FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE column = 'a' ORDER BY part_name;
SELECT 'where substream';
SELECT part_name, substream FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE substream = 'b' ORDER BY part_name;
SELECT 'where part_name and column';
SELECT part_name, column FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE part_name = 'all_2_2_0' AND column = 'b';

SELECT 'where column and map, only column is pushed down';
SELECT part_name, column, mapKeys(codec_block_counts) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE column = 'a' AND mapContains(codec_block_counts, 'LZ4') ORDER BY part_name;
SELECT 'where column and map, no match';
SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE column = 'a' AND mapContains(codec_block_counts, 'ZSTD');

SELECT 'where map only, nothing is pushed down';
SELECT part_name, column FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE mapContains(codec_block_counts, 'ZSTD(1)') ORDER BY part_name;

SELECT 'where column or map, nothing is pushed down';
SELECT part_name, column FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE column = 'a' OR mapContains(codec_block_counts, 'ZSTD(1)') ORDER BY part_name, column;

SELECT 'where column or substream, whole or is pushed down';
SELECT part_name, column FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE column = 'b' OR substream = 'b' ORDER BY part_name;

SELECT 'where part_name in';
SELECT part_name, column FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE part_name IN ('all_1_1_0', 'no_such_part') ORDER BY column;

SELECT 'where part_name matches nothing';
SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE part_name = 'no_such_part';

-- max_threads = 1: with more the opened-file cache makes FileOpen nondeterministic.
SELECT 'codec sums: baseline, column, mixed, part_name, in, substream, or, unpushed or';
SELECT sum(length(codec_block_counts)) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) SETTINGS max_threads = 1, log_comment = '04507_codec_counts_baseline';
SELECT sum(length(codec_block_counts)) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE column = 'a' SETTINGS max_threads = 1, log_comment = '04507_codec_counts_column';
SELECT sum(length(codec_block_counts)) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE column = 'a' AND mapContains(codec_block_counts, 'LZ4') SETTINGS max_threads = 1, log_comment = '04507_codec_counts_mixed';
SELECT sum(length(codec_block_counts)) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE part_name = 'all_1_1_0' SETTINGS max_threads = 1, log_comment = '04507_codec_counts_part';
SELECT sum(length(codec_block_counts)) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE part_name IN ('all_1_1_0', 'no_such_part') SETTINGS max_threads = 1, log_comment = '04507_codec_counts_in';
SELECT sum(length(codec_block_counts)) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE substream = 'b' SETTINGS max_threads = 1, log_comment = '04507_codec_counts_substream';
SELECT sum(length(codec_block_counts)) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE column = 'b' OR substream = 'b' SETTINGS max_threads = 1, log_comment = '04507_codec_counts_or';
SELECT sum(length(codec_block_counts)) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_pushdown) WHERE column = 'a' OR mapContains(codec_block_counts, 'ZSTD(1)') SETTINGS max_threads = 1, log_comment = '04507_codec_counts_or_unpushed';

SYSTEM FLUSH LOGS query_log;

SELECT 'fewer files than baseline: column, mixed, part_name, in, substream, or. as many as baseline: unpushed or';
SELECT
    maxIf(file_open, log_comment = '04507_codec_counts_column') < maxIf(file_open, log_comment = '04507_codec_counts_baseline'),
    maxIf(file_open, log_comment = '04507_codec_counts_mixed') < maxIf(file_open, log_comment = '04507_codec_counts_baseline'),
    maxIf(file_open, log_comment = '04507_codec_counts_part') < maxIf(file_open, log_comment = '04507_codec_counts_baseline'),
    maxIf(file_open, log_comment = '04507_codec_counts_in') < maxIf(file_open, log_comment = '04507_codec_counts_baseline'),
    maxIf(file_open, log_comment = '04507_codec_counts_substream') < maxIf(file_open, log_comment = '04507_codec_counts_baseline'),
    maxIf(file_open, log_comment = '04507_codec_counts_or') < maxIf(file_open, log_comment = '04507_codec_counts_baseline'),
    maxIf(file_open, log_comment = '04507_codec_counts_or_unpushed') = maxIf(file_open, log_comment = '04507_codec_counts_baseline')
FROM
(
    SELECT log_comment, ProfileEvents['FileOpen'] AS file_open
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment LIKE '04507_codec_counts_%'
);

DROP TABLE t_pushdown;
