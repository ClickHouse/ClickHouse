-- Regression test: boolean wrappers around predicates supported by the experimental
-- cuckoo_filter / binary_fuse_filter skip indexes must not collapse to the positive
-- inner atom and skip granules that actually satisfy the wrapper (false negatives).
-- The extractor only recurses into children of a top-level function for the
-- indexOf() case, and indexOf() itself is only usable when the surrounding
-- comparison implies the array contains the element.

SET allow_experimental_cuckoo_filter_index = 1;
SET allow_experimental_binary_fuse_filter_index = 1;

DROP TABLE IF EXISTS tab_cuckoo_wrapper;
CREATE TABLE tab_cuckoo_wrapper
(
    k UInt64,
    arr Array(UInt64),
    m Map(String, UInt64),
    INDEX idx_k k TYPE cuckoo_filter(0.025) GRANULARITY 1,
    INDEX idx_arr arr TYPE cuckoo_filter(0.025) GRANULARITY 1,
    INDEX idx_map mapKeys(m) TYPE cuckoo_filter(0.025) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;

-- Rows 0..7 contain the "hit" values (1 in arr, 'key' in m); rows 8..15 do not.
-- With index_granularity = 4 the second half occupies whole granules, so a
-- wrongly-extracted positive atom would prune exactly the rows that match the wrapper.
INSERT INTO tab_cuckoo_wrapper
SELECT
    number,
    if(number < 8, [1, number], [100 + number]),
    if(number < 8, map('key', number), map('other', number))
FROM numbers(16);

SELECT 'cuckoo_filter';
-- Positive sanity: the index may prune here, results must stay correct.
SELECT count() FROM tab_cuckoo_wrapper WHERE has(arr, 1);
SELECT count() FROM tab_cuckoo_wrapper WHERE mapContains(m, 'key');
-- Boolean wrappers around indexed atoms: must not be treated as the inner atom.
SELECT count() FROM tab_cuckoo_wrapper WHERE has(arr, 1) = 0;
SELECT count() FROM tab_cuckoo_wrapper WHERE NOT has(arr, 1);
SELECT count() FROM tab_cuckoo_wrapper WHERE mapContains(m, 'key') = 0;
SELECT count() FROM tab_cuckoo_wrapper WHERE (k = 3) = 0;
SELECT count() FROM tab_cuckoo_wrapper WHERE (k = 42) = 0;
-- indexOf() inside a comparison that does NOT imply membership must not prune.
SELECT count() FROM tab_cuckoo_wrapper WHERE indexOf(arr, 1) = 0;
SELECT count() FROM tab_cuckoo_wrapper WHERE indexOf(arr, 1) <= 0;
-- ... while membership-implying comparisons stay correct.
SELECT count() FROM tab_cuckoo_wrapper WHERE indexOf(arr, 1) != 0;
SELECT count() FROM tab_cuckoo_wrapper WHERE indexOf(arr, 1) > 0;

DROP TABLE tab_cuckoo_wrapper;

DROP TABLE IF EXISTS tab_bfuse_wrapper;
CREATE TABLE tab_bfuse_wrapper
(
    k UInt64,
    arr Array(UInt64),
    m Map(String, UInt64),
    INDEX idx_k k TYPE binary_fuse_filter(0.025) GRANULARITY 1,
    INDEX idx_arr arr TYPE binary_fuse_filter(0.025) GRANULARITY 1,
    INDEX idx_map mapKeys(m) TYPE binary_fuse_filter(0.025) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;

INSERT INTO tab_bfuse_wrapper
SELECT
    number,
    if(number < 8, [1, number], [100 + number]),
    if(number < 8, map('key', number), map('other', number))
FROM numbers(16);

SELECT 'binary_fuse_filter';
SELECT count() FROM tab_bfuse_wrapper WHERE has(arr, 1);
SELECT count() FROM tab_bfuse_wrapper WHERE mapContains(m, 'key');
SELECT count() FROM tab_bfuse_wrapper WHERE has(arr, 1) = 0;
SELECT count() FROM tab_bfuse_wrapper WHERE NOT has(arr, 1);
SELECT count() FROM tab_bfuse_wrapper WHERE mapContains(m, 'key') = 0;
SELECT count() FROM tab_bfuse_wrapper WHERE (k = 3) = 0;
SELECT count() FROM tab_bfuse_wrapper WHERE (k = 42) = 0;
SELECT count() FROM tab_bfuse_wrapper WHERE indexOf(arr, 1) = 0;
SELECT count() FROM tab_bfuse_wrapper WHERE indexOf(arr, 1) <= 0;
SELECT count() FROM tab_bfuse_wrapper WHERE indexOf(arr, 1) != 0;
SELECT count() FROM tab_bfuse_wrapper WHERE indexOf(arr, 1) > 0;

DROP TABLE tab_bfuse_wrapper;
