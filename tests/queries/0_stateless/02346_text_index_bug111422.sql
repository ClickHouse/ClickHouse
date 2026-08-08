-- Tags: no-old-analyzer

-- Text-index direct read left an unused virtual column in the ReadFromMergeTree output header,
-- which desynced positional bookkeeping in optimizeLazyMaterialization2 and caused an out-of-bounds
-- read (release) / assertion (debug). See issue #111422.

SET enable_full_text_index = 1;

-- Pin the settings that select this code path; the test runner randomizes all of them and would
-- otherwise skip lazy materialization (small max limit) or the text-index direct read, letting a
-- broken build pass.
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100000;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    map Map(String, String),
    INDEX idx_mv mapValues(map) TYPE text(tokenizer = 'array') GRANULARITY 100000000,
    INDEX idx_mk mapKeys(map)   TYPE text(tokenizer = 'array') GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO tab VALUES (1, {'127.0.0.1':'a', '::1':'b'}), (2, {'x':'y'}), (3, {'(':'z'});

-- Two text indexes + PREWHERE and WHERE on different Map subscripts + LIMIT triggers the lazy
-- materialization pass. No matching row: must return nothing (previously crashed the server).
SELECT id FROM tab
PREWHERE '(' IN (map['127.0.0.1'])
WHERE map['::1'] IN '('
ORDER BY ALL
LIMIT 681;

-- Same shape with a matching row: the result must be correct, not merely non-crashing.
SELECT id FROM tab
PREWHERE 'a' IN (map['127.0.0.1'])
WHERE map['::1'] IN 'b'
ORDER BY ALL
LIMIT 681;

DROP TABLE tab;

SELECT '-- the lazy materialization pass is reached (plan oracle)';

-- The two queries above assert results only, so a refactor that stops routing their shape
-- into the lazy materialization pass would unarm them silently. This group asserts the plan
-- shape instead. It needs a deferrable non-key column, which `tab` does not have.
DROP TABLE IF EXISTS tab_lazy;

CREATE TABLE tab_lazy
(
    id UInt32,
    pad String,
    map Map(String, String),
    INDEX idx_mv mapValues(map) TYPE text(tokenizer = 'array') GRANULARITY 100000000,
    INDEX idx_mk mapKeys(map)   TYPE text(tokenizer = 'array') GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
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

DROP TABLE tab_lazy;
