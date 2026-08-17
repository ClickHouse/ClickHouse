-- Regression tests for specific bugs found while developing the keyValuePairs text-index tokenizer.
-- Each block reproduces one fixed bug; feature completeness lives in 04726_map_kv_tokenizer_basic.

-- ============================================================================
-- Bug: the multi-byte token trailer was decoded with a scan bounded by the token size instead of the
-- packed length. The trailer encodes (length(key) << 1) | is_rest, so a 64-byte key (packed 128 ->
-- 2 trailer bytes) with an empty/short value keeps the whole token under 128 bytes; bounding the
-- backward scan by the token size stopped before the terminator and mis-decoded to 0, silently missing
-- rows on every decode-scan query (mapContainsKey/Value, m['key'] LIKE/startsWith/endsWith). The bound
-- must come from the packed length. A 63-byte key (packed 126) is the last single-trailer-byte case.
-- ============================================================================
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;
CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_mem VALUES
    (1, map(repeat('a', 64), '')),   -- 64-byte key (2-byte trailer), empty value: short total token
    (2, map(repeat('b', 63), 'c'));  -- 63-byte key: last single-trailer-byte boundary
INSERT INTO t_idx SELECT * FROM t_mem;

SELECT '-- multi-byte trailer in a short token: decode-scan must not mis-read --';
SELECT id FROM t_mem WHERE mapContainsKey(m, repeat('a', 64)) ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKey(m, repeat('a', 64)) ORDER BY id;
SELECT id FROM t_mem WHERE mapContainsKey(m, repeat('b', 63)) ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKey(m, repeat('b', 63)) ORDER BY id;
SELECT id FROM t_mem WHERE mapContainsValue(m, 'c') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsValue(m, 'c') ORDER BY id;
SELECT id FROM t_mem WHERE mapContainsKeyValue(m, repeat('a', 64), '') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValue(m, repeat('a', 64), '') ORDER BY id;
DROP TABLE t_mem;
DROP TABLE t_idx;

-- ============================================================================
-- Bug: a value-matcher query (e.g. mapContainsValue) discovers its tokens dynamically and was failed
-- by the first clipped token. PK filtering (id >= 7) prunes the granule holding the earlier-sorted
-- token ('k2' -> 'x' from row 1), making it unreadable; the query must still match row 8 via its own
-- ('k' -> 'x') token instead of reporting a false negative (empty result).
-- ============================================================================
DROP TABLE IF EXISTS t_map_kv_clip;
CREATE TABLE t_map_kv_clip
(
    id UInt64,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;
INSERT INTO t_map_kv_clip VALUES
    (1, {'k2':'x'}), (2, {'z':'q'}), (3, {'z':'q'}), (4, {'z':'q'}),
    (5, {'z':'q'}), (6, {'z':'q'}), (7, {'z':'q'}), (8, {'k':'x'});
SELECT id FROM t_map_kv_clip WHERE id >= 7 AND mapContainsValue(m, 'x') ORDER BY id;
DROP TABLE t_map_kv_clip;

-- ============================================================================
-- Bug: a false negative in mapContainsKeyValue over a keyValuePairs text index. A (key, value) pair is
-- stored as either the first-occurrence token (is_rest = 0) or a later-occurrence token (is_rest = 1),
-- so mapContainsKeyValue is the union of the two variants. Representing that union as a single
-- two-token FUNCTION_EQUALS made granule pruning (and direct read) treat the partially folded posting
-- list as complete: while one variant's postings were still unread, a mark holding only that variant
-- was wrongly pruned. It must instead be an OR over the two variants (FUNCTION_HAS_ANY_ELEMENTS), so
-- all postings are read and the union is exact. The unread-variant condition arises once a variant's
-- posting list spans more than one posting block; blocks split on roaring-container boundaries
-- (65536 row ids), so the later-duplicate rows are placed in two containers and
-- text_index_posting_list_block_size is small. Before the fix the index returned 11 instead of 19.
-- ============================================================================
DROP TABLE IF EXISTS t_mb;
CREATE TABLE t_mb (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, text_index_posting_list_block_size = 4;
INSERT INTO t_mb SELECT number, multiIf(
    number < 8, map('dup', 'y', 'dup', 'x'),                             -- ('dup','x') as later-dup, roaring container 0
    number >= 65536 AND number < 65544, map('dup', 'y', 'dup', 'x'),     -- ('dup','x') as later-dup, roaring container 1
    number >= 100 AND number < 103, map('dup', 'x'),                     -- ('dup','x') as first occurrence (embedded posting)
    map('other', 'z'))
    FROM numbers(65544);

SELECT '-- mapContainsKeyValue: index must equal brute-force (19) on the pruning and direct-read paths --';
SELECT count() FROM t_mb WHERE mapContainsKeyValue(m, 'dup', 'x') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_mb WHERE mapContainsKeyValue(m, 'dup', 'x') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_mb WHERE mapContainsKeyValue(m, 'dup', 'x') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT '-- the absent pair must still match nothing --';
SELECT count() FROM t_mb WHERE mapContainsKeyValue(m, 'dup', 'absent') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE t_mb;

-- ============================================================================
-- Bug: exact direct read of m['key'] IN (...) on a part where the index is NOT materialized dropped
-- rows. Exact mode rebuilds the predicate to fill the virtual column and removes the original; the IN
-- set (a ColumnSet) did not round-trip and degraded to a NULL literal, so the non-materialized part
-- returned nothing. The fallback must be rebuilt as m['key'] = v1 OR ... OR m['key'] = vn. Both the
-- non-subcolumn and subcolumn accessor forms must return every matching row.
-- ============================================================================
SELECT '-- regression: a part where the index is not materialized must not drop rows --';
DROP TABLE IF EXISTS t_mix;
CREATE TABLE t_mix (id UInt64, m Map(String, String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_mix VALUES (10, map('lvl', 'err')), (11, map('lvl', 'warn')), (12, map('lvl', 'info'));   -- part left non-materialized
ALTER TABLE t_mix ADD INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1;
INSERT INTO t_mix VALUES (1, map('lvl', 'err')), (2, map('lvl', 'warn')), (3, map('lvl', 'debug'));      -- part materialized (index built on insert)
SELECT id FROM t_mix WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 0;
SELECT '-- regression (subcolumn form): non-materialized part must not drop rows --';
SELECT id FROM t_mix WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 1;
DROP TABLE t_mix;

-- ============================================================================
-- Bug: SerializationMapKeyValue::deserializeBinaryBulkWithMultipleStreams with reading_full_map = false
-- (only subcolumns, not the full Map), Wide part, and both m.size0 and m.key_a read together. m.size0
-- caches offsets in the substreams cache; when m.key_a's Array serialization reads offsets it finds the
-- cached column. On the second readRows() call the cached offsets held ALL accumulated rows, not just the
-- current range, so the Array read too many elements. The fix limits the cache to the current range for
-- Wide parts. Non-contiguous mark ranges (a WHERE gap on the primary key) force multiple readRows() calls.
-- ============================================================================
DROP TABLE IF EXISTS t_map_cache_bug;
CREATE TABLE t_map_cache_bug (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types',
    index_granularity = 700;

INSERT INTO t_map_cache_bug SELECT number, multiIf(
    number < 9000, map('a', number, 'b', number + 1, 'c', number + 2),
    number < 18000, map('a', number, 'b', number + 1),
    number < 27000, map('c', number, 'd', number + 1),
    number < 36000, map('a', number),
    number < 42000, map('a', number, 'b', number + 1, 'c', number + 2, 'd', number + 3, 'e', number + 4),
    map('b', number)
) FROM numbers(45000);

-- WHERE creates non-contiguous mark ranges with a gap, forcing two readRows() calls for the same
-- result columns. max_threads = 1 ensures a single thread processes all ranges in one block.
SELECT m.size0, m.key_a FROM t_map_cache_bug WHERE id < 3500 OR id >= 7000 FORMAT Null SETTINGS max_threads = 1;
-- Also test the full table scan (single contiguous range, should always work).
SELECT m.size0, m.key_a FROM t_map_cache_bug FORMAT Null SETTINGS max_threads = 1;

DROP TABLE t_map_cache_bug;
