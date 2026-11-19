-- A text index on `mapKeys` or `JSONAllPaths` evaluates the predicate on the value of a missing key or path.
-- If the predicate throws on it, the index cannot answer the predicate, and the query must not fail.

SET enable_analyzer = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_map;
DROP TABLE IF EXISTS tab_json;

CREATE TABLE tab_map
(
    id UInt32,
    m Map(String, String),
    INDEX idx mapKeys(m) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_json
(
    id UInt32,
    d JSON(TypeName Nullable(String)),
    INDEX idx JSONAllPaths(d) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_map VALUES (1, {'status': '200'}), (2, {'status': '404'});
INSERT INTO tab_json VALUES (1, '{"TypeName": "User"}'), (2, '{"TypeName": "Group"}');

SELECT '-- map';
SELECT id FROM tab_map WHERE toUInt64(m['status']) = 404 ORDER BY id;
SELECT id FROM tab_map WHERE toUInt64(m['status']) = 404 ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM tab_map WHERE hasToken(toString(toUInt64(m['status'])), '404') ORDER BY id;
SELECT id FROM tab_map WHERE hasToken(toString(toUInt64(m['status'])), '404') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- JSON';
SELECT id FROM tab_json WHERE d.TypeName::String = 'User' ORDER BY id;
SELECT id FROM tab_json WHERE d.TypeName::String = 'User' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM tab_json WHERE hasToken(d.TypeName::String, 'User') ORDER BY id;
SELECT id FROM tab_json WHERE hasToken(d.TypeName::String, 'User') ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab_map;
DROP TABLE tab_json;
