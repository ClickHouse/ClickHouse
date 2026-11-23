-- Tests `m['key'] IN (subquery)` on a `keyValuePairs` text index when the set is `FixedString` or `Nullable`.

DROP TABLE IF EXISTS tab;
SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab VALUES (1, {'level':'error'}), (2, {'level':'warn'}), (3, {'level':'error'}), (4, {'level':'info'});

-- IN casts the map element to the set's FixedString type, so 'error' matches the padded element.
SELECT 'fixed string idx', id FROM tab WHERE m['level'] IN (SELECT toFixedString('error', 8)) ORDER BY id;
SELECT 'fixed string scan', id FROM tab WHERE m['level'] IN (SELECT toFixedString('error', 8)) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'fixed string skip index', count() FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE m['level'] IN (SELECT toFixedString('error', 8))) WHERE explain LIKE '%Name: idx%';
SELECT 'string skip index', count() FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE m['level'] IN (SELECT 'error')) WHERE explain LIKE '%Name: idx%';

SELECT 'nullable idx', id FROM tab WHERE m['level'] IN (SELECT arrayJoin([toNullable('warn'), NULL])) ORDER BY id SETTINGS transform_null_in = 1;
SELECT 'nullable scan', id FROM tab WHERE m['level'] IN (SELECT arrayJoin([toNullable('warn'), NULL])) ORDER BY id SETTINGS transform_null_in = 1, use_skip_indexes = 0;
SELECT 'nullable skip index', count() FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE m['level'] IN (SELECT arrayJoin([toNullable('warn'), NULL])) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Name: idx%';
SELECT 'nullable without null skip index', count() FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE m['level'] IN (SELECT toNullable('warn')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Name: idx%';

DROP TABLE tab;
