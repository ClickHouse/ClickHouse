-- Tags: no-fasttest, use-vectorscan

-- Tests multiSearchAny, its variants and multiMatchAny over a JSON path with a text index on JSONAllValues: the rows match a full scan.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET text_index_like_min_pattern_length = 4;
SET use_query_condition_cache = 0;
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS tab_json;

CREATE TABLE tab_json
(
    id UInt32,
    json JSON(t String),
    INDEX idx(JSONAllValues(json)) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

-- The index holds the values of every path, so row 2 has the needle in the index but not under json.t.
INSERT INTO tab_json VALUES
    (1, '{"t":"java.lang.NullPointerException at Main"}'),
    (2, '{"t":"all good","b":"java.lang.NullPointerException"}'),
    (3, '{"t":"nullpointerexception in lower case"}'),
    (4, '{"b":"x"}');

SELECT 'multiSearchAny', arraySort(groupArray(id)) FROM tab_json WHERE multiSearchAny(json.t, ['PointerExc']);
SELECT 'multiSearchAny, no index', arraySort(groupArray(id)) FROM tab_json WHERE multiSearchAny(json.t, ['PointerExc']) SETTINGS use_skip_indexes = 0;
SELECT 'multiSearchAnyUTF8', arraySort(groupArray(id)) FROM tab_json WHERE multiSearchAnyUTF8(json.t, ['PointerExc']);
SELECT 'multiSearchAnyUTF8, no index', arraySort(groupArray(id)) FROM tab_json WHERE multiSearchAnyUTF8(json.t, ['PointerExc']) SETTINGS use_skip_indexes = 0;
SELECT 'multiSearchAnyCaseInsensitive', arraySort(groupArray(id)) FROM tab_json WHERE multiSearchAnyCaseInsensitive(json.t, ['POINTEREXC']);
SELECT 'multiSearchAnyCaseInsensitive, no index', arraySort(groupArray(id)) FROM tab_json WHERE multiSearchAnyCaseInsensitive(json.t, ['POINTEREXC']) SETTINGS use_skip_indexes = 0;
SELECT 'multiSearchAnyCaseInsensitiveUTF8', arraySort(groupArray(id)) FROM tab_json WHERE multiSearchAnyCaseInsensitiveUTF8(json.t, ['POINTEREXC']);
SELECT 'multiSearchAnyCaseInsensitiveUTF8, no index', arraySort(groupArray(id)) FROM tab_json WHERE multiSearchAnyCaseInsensitiveUTF8(json.t, ['POINTEREXC']) SETTINGS use_skip_indexes = 0;
SELECT 'multiMatchAny', arraySort(groupArray(id)) FROM tab_json WHERE multiMatchAny(json.t, ['PointerExc']);
SELECT 'multiMatchAny, no index', arraySort(groupArray(id)) FROM tab_json WHERE multiMatchAny(json.t, ['PointerExc']) SETTINGS use_skip_indexes = 0;
SELECT 'CAST', arraySort(groupArray(id)) FROM tab_json WHERE multiSearchAny(json.b::String, ['PointerExc']);
SELECT 'CAST, no index', arraySort(groupArray(id)) FROM tab_json WHERE multiSearchAny(json.b::String, ['PointerExc']) SETTINGS use_skip_indexes = 0;

-- The index serves hasToken on the same path, but not multiSearchAny.
SELECT 'hasToken uses the index', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_json WHERE hasToken(json.t, 'Main')) WHERE explain LIKE '%Name: idx%';
SELECT 'multiSearchAny uses the index', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_json WHERE multiSearchAny(json.t, ['Main'])) WHERE explain LIKE '%Name: idx%';

DROP TABLE tab_json;
