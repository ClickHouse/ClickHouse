-- The whole text column was read instead of the index. The trigger is the
-- column order of the read header: `SELECT id, v ... ORDER BY v` regressed, `SELECT v, id` did not.

SET query_plan_direct_read_from_text_index = 1;
SET use_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET query_plan_merge_expressions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    v UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, number % 1000, if (number % 100 = 0, 'error', 'fine') FROM numbers(10000);

-- Guard: the top-K optimization must still apply, or the test stops covering the case.
SELECT 'top-K applied', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT id, v FROM tab WHERE hasToken(s, 'error') ORDER BY v DESC, id LIMIT 10
)
WHERE explain LIKE '%__topKFilter%';

-- Every plan below must reference the `__text_index_*` virtual column.
SELECT 'aggregate over subquery', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM (SELECT id, v FROM tab WHERE hasToken(s, 'error') ORDER BY v DESC, id LIMIT 10)
)
WHERE explain LIKE '%__text_index_idx_s_hasToken%';

SELECT 'plain select', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT id, v FROM tab WHERE hasToken(s, 'error') ORDER BY v DESC, id LIMIT 10
)
WHERE explain LIKE '%__text_index_idx_s_hasToken%';

SELECT 'single sort column', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT id, v FROM tab WHERE hasToken(s, 'error') ORDER BY v DESC LIMIT 10
)
WHERE explain LIKE '%__text_index_idx_s_hasToken%';

SELECT 'limit with offset', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT id, v FROM tab WHERE hasToken(s, 'error') ORDER BY v DESC, id LIMIT 100 OFFSET 100
)
WHERE explain LIKE '%__text_index_idx_s_hasToken%';

SELECT 'sort column selected first', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT v, id FROM tab WHERE hasToken(s, 'error') ORDER BY v DESC, id LIMIT 10
)
WHERE explain LIKE '%__text_index_idx_s_hasToken%';

SELECT 'results', sum(id), sum(v) FROM (SELECT id, v FROM tab WHERE hasToken(s, 'error') ORDER BY v DESC, id LIMIT 10);
SELECT 'results reference', sum(id), sum(v) FROM (SELECT id, v FROM tab WHERE hasToken(s, 'error') ORDER BY v DESC, id LIMIT 10 SETTINGS query_plan_direct_read_from_text_index = 0, use_top_k_dynamic_filtering = 0);

DROP TABLE tab;
