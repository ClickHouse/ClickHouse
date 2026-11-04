-- A merge drops a column whose every value has expired by its TTL without reading it. A text index on such a
-- column must not be taken from the source parts, which describe the values before they expired: readers see
-- the default value of the column in the merged part, so the index would give wrong results.
-- The first case merges two parts, so the merge is a regular one rather than a single-part `CLEAR COLUMN`.

SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_ttl_text_index;

CREATE TABLE t_ttl_text_index
(
    d Date,
    key UInt64,
    s String DEFAULT 'foo' TTL d + INTERVAL 1 DAY,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY key
SETTINGS max_number_of_merges_with_ttl_in_pool = 0;

SYSTEM STOP MERGES t_ttl_text_index;

INSERT INTO t_ttl_text_index SELECT '2020-01-01', number, 'bar' FROM numbers(100);
INSERT INTO t_ttl_text_index SELECT '2020-01-01', number + 100, 'bar' FROM numbers(100);

SYSTEM START MERGES t_ttl_text_index;
OPTIMIZE TABLE t_ttl_text_index FINAL;

SELECT count(), countIf(s = 'foo') FROM t_ttl_text_index;
SELECT count() FROM t_ttl_text_index WHERE hasToken(s, 'foo');
SELECT count() FROM t_ttl_text_index WHERE hasToken(s, 'bar');

DROP TABLE t_ttl_text_index;

-- A single part: the merge only drops the column and hardlinks the other files, while the text index is
-- rebuilt from the default value of the column.
DROP TABLE IF EXISTS t_ttl_text_index_single;

CREATE TABLE t_ttl_text_index_single
(
    d Date,
    key UInt64,
    s String DEFAULT 'foo' TTL d + INTERVAL 1 DAY,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_number_of_merges_with_ttl_in_pool = 0;

INSERT INTO t_ttl_text_index_single SELECT '2020-01-01', number, 'bar' FROM numbers(100);
OPTIMIZE TABLE t_ttl_text_index_single FINAL;

SELECT count(), countIf(s = 'foo') FROM t_ttl_text_index_single;
SELECT count() FROM t_ttl_text_index_single WHERE hasToken(s, 'foo');
SELECT count() FROM t_ttl_text_index_single WHERE hasToken(s, 'bar');
SELECT secondary_indices_uncompressed_bytes > 0 FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_text_index_single' AND active;

SYSTEM FLUSH LOGS part_log;
SELECT ProfileEvents['MutationSomePartColumns'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_ttl_text_index_single' AND event_type = 'MergeParts';

DROP TABLE t_ttl_text_index_single;
