-- Tags: no-parallel-replicas
-- Two regexp arrays whose patterns concatenate to the same string are different searches.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'abc'), (2, 'xf');

SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokenRegexp(msg, ['a.c', 'd.f']) AND hasAnyTokenRegexp(msg, ['a.cd', '.f']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokenRegexp(msg, ['a.c', 'd.f']) OR hasAnyTokenRegexp(msg, ['a.cd', '.f']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokenRegexp(msg, ['a.c', 'd.f']) AND hasAnyTokenRegexp(msg, ['a.cd', '.f']) ORDER BY id) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokenRegexp(msg, ['a.c', 'd.f']) OR hasAnyTokenRegexp(msg, ['a.cd', '.f']) ORDER BY id) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAnyTokenRegexp(msg, ['a.c', 'd.f']) OR hasAnyTokenRegexp(msg, ['a.cd', '.f']) ORDER BY id) SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
