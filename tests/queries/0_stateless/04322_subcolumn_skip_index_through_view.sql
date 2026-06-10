-- Tags: no-random-merge-tree-settings
-- Subcolumn skip indexes and primary key must be used for subcolumn predicates that arrive through a view,
-- the same as for the base table. Through a view the analyzer rewrites `col.sub` into
-- `getSubcolumn(col, 'sub')`; index analysis must fold that back to the canonical subcolumn name.
-- https://github.com/ClickHouse/ClickHouse/issues/107038

SET use_query_condition_cache = 0;
SET parallel_replicas_index_analysis_only_on_coordinator = 0;

DROP TABLE IF EXISTS tab;
DROP VIEW IF EXISTS v;

CREATE TABLE tab
(
    id UInt64,
    arr Array(Tuple(role String, text String)),
    tup Tuple(role String, text String),
    INDEX idx_text arr.text TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1,
    INDEX idx_arr_bf arr.text TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tup_bf tup.text TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO tab SELECT number, [('user', 'lorem ipsum filler ' || toString(number))], ('user', 'tup filler ' || toString(number)) FROM numbers(64);
INSERT INTO tab VALUES (64, [('user', 'the needle token')], ('user', 'tup needle'));

CREATE VIEW v AS SELECT * FROM tab;

-- text index on Array(Tuple).text subcolumn, through the view: index must be used (query errors otherwise),
-- and the result must be correct.
SELECT count() FROM v WHERE hasAnyTokens(arr.text, ['needle']) SETTINGS force_data_skipping_indices = 'idx_text';

-- generic bloom_filter on the same Array(Tuple).text subcolumn, through the view.
SELECT count() FROM v WHERE has(arr.text, 'the needle token') SETTINGS force_data_skipping_indices = 'idx_arr_bf';

-- generic bloom_filter on a plain Tuple subcolumn, through the view.
SELECT count() FROM v WHERE tup.text = 'tup needle' SETTINGS force_data_skipping_indices = 'idx_tup_bf';

-- The skip section must be present (and prune) in the plan when querying through the view,
-- matching the direct-table behaviour.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasAnyTokens(arr.text, ['needle'])) WHERE explain LIKE '%Name:%' OR explain LIKE '%Skip%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM v   WHERE hasAnyTokens(arr.text, ['needle'])) WHERE explain LIKE '%Name:%' OR explain LIKE '%Skip%';

DROP VIEW v;
DROP TABLE tab;

-- Primary key on a (nested) Tuple subcolumn must also prune through a view.
DROP TABLE IF EXISTS tab_pk;
DROP VIEW IF EXISTS v_pk;

CREATE TABLE tab_pk (id UInt64, t Tuple(inner Tuple(x UInt64, y String)))
ENGINE = MergeTree ORDER BY t.inner.x SETTINGS index_granularity = 4;

INSERT INTO tab_pk SELECT number, tuple(tuple(number, toString(number))) FROM numbers(256);

CREATE VIEW v_pk AS SELECT * FROM tab_pk;

-- Same primary-key condition and granule pruning whether read directly or through the view.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_pk WHERE t.inner.x = 100) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM v_pk   WHERE t.inner.x = 100) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';
SELECT count() FROM v_pk WHERE t.inner.x = 100;

DROP VIEW v_pk;
DROP TABLE tab_pk;
