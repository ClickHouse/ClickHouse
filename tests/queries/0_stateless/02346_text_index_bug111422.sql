-- Tags: no-old-analyzer

-- Reads a Map through two text indexes with lazy materialization enabled. See issue #111422.

SET enable_full_text_index = 1;

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100000;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    map Map(String, String),
    INDEX idx_mv mapValues(map) TYPE text(tokenizer = 'array'),
    INDEX idx_mk mapKeys(map) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO tab VALUES (1, {'127.0.0.1':'a', '::1':'b'}), (2, {'x':'y'}), (3, {'(':'z'});

SELECT '-- PREWHERE and WHERE on different Map subscripts, no row matches';

SELECT id FROM tab
PREWHERE '(' IN (map['127.0.0.1'])
WHERE map['::1'] IN '('
ORDER BY ALL
LIMIT 681;

SELECT '-- same shape, one row matches';

SELECT id FROM tab
PREWHERE 'a' IN (map['127.0.0.1'])
WHERE map['::1'] IN 'b'
ORDER BY ALL
LIMIT 681;

DROP TABLE tab;

SELECT '-- the lazy materialization pass is reached (plan oracle)';

DROP TABLE IF EXISTS tab_lazy;

CREATE TABLE tab_lazy
(
    id UInt32,
    pad String,
    map Map(String, String),
    INDEX idx_mv mapValues(map) TYPE text(tokenizer = 'array'),
    INDEX idx_mk mapKeys(map) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO tab_lazy VALUES (1, 'p', {'127.0.0.1':'a', '::1':'b'}), (2, 'q', {'x':'y'});

SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0,
       countIf(explain LIKE '%__text_index_%') > 0
FROM (
    EXPLAIN actions = 1
    SELECT id, pad FROM tab_lazy
    PREWHERE 'a' IN (map['127.0.0.1'])
    WHERE map['::1'] IN 'b'
    ORDER BY id
    LIMIT 681
);

SELECT '-- in the plan, mapKeys virtual is present and mapValues virtual is absent (EXPLAIN entry path)';

SELECT countIf(explain LIKE '%__text_index_idx_mk%') > 0,
       countIf(explain LIKE '%__text_index_idx_mv%')
FROM (
    EXPLAIN actions = 1
    SELECT id, pad FROM tab_lazy
    PREWHERE 'a' IN (map['127.0.0.1'])
    WHERE map['::1'] IN 'b'
    ORDER BY id
    LIMIT 681
);

DROP TABLE tab_lazy;
