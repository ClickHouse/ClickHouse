-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read from the text index is not compatible with parallel replicas

-- A text index answers the merged `hasAny` once per block, and the direct read serves it as one hint column.
SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET use_skip_indexes_on_data_read = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_optimize_count_from_text_index = 0;
SET optimize_rewrite_has_chain_to_has_any = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, words Array(String), INDEX ix words TYPE text(tokenizer = array) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
INSERT INTO tab SELECT number, arrayMap(i -> concat('w', toString((number * 7 + i) % 10)), range(3)) FROM numbers(64);

-- The chain returns the same rows as the hand-written `hasAny`, with and without the direct read and the rewrite.
SELECT count() FROM tab WHERE NOT has(words, 'w1') AND NOT has(words, 'w2') AND NOT has(words, 'w3') AND NOT has(words, 'w4');
SELECT count() FROM tab WHERE NOT has(words, 'w1') AND NOT has(words, 'w2') AND NOT has(words, 'w3') AND NOT has(words, 'w4') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE NOT has(words, 'w1') AND NOT has(words, 'w2') AND NOT has(words, 'w3') AND NOT has(words, 'w4') SETTINGS optimize_rewrite_has_chain_to_has_any = 0;
SELECT count() FROM tab WHERE NOT hasAny(words, ['w1', 'w2', 'w3', 'w4']);
SELECT id FROM tab WHERE NOT has(words, 'w1') AND NOT has(words, 'w2') AND NOT has(words, 'w3') AND NOT has(words, 'w4') ORDER BY id LIMIT 5;

-- The direct read serves the chain as one `hasAny` hint column; without the rewrite it needs one `has` hint column per word.
-- The index description in `EXPLAIN indexes = 1` lists the tokens of all conditions together, so it does not tell the two apart.
SELECT countIf(explain LIKE '%INPUT%\_\_text_index_ix_hasAny%'), countIf(explain LIKE '%INPUT%\_\_text_index_ix_has\_%')
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE NOT has(words, 'w1') AND NOT has(words, 'w2') AND NOT has(words, 'w3') AND NOT has(words, 'w4') SETTINGS query_plan_remove_unused_columns = 1);
SELECT countIf(explain LIKE '%INPUT%\_\_text_index_ix_hasAny%'), countIf(explain LIKE '%INPUT%\_\_text_index_ix_has\_%')
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE NOT has(words, 'w1') AND NOT has(words, 'w2') AND NOT has(words, 'w3') AND NOT has(words, 'w4') SETTINGS query_plan_remove_unused_columns = 1, optimize_rewrite_has_chain_to_has_any = 0);

DROP TABLE tab;
