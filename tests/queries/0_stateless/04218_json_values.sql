-- Tests for JSONValues function

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    data JSON(max_dynamic_paths=2),
    INDEX idx JSONValues(data, ['type.name', 'player.name']) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (0, '{"type": {"name": "goal"}, "player": {"name": "Salah"}, "score": 1}');
INSERT INTO tab VALUES (1, '{"type": {"name": "assist"}, "player": {"name": "Trent"}, "score": 0}');
INSERT INTO tab VALUES (2, '{"type": {"name": "goal"}, "player": {"name": "Firmino"}, "score": 2}');
INSERT INTO tab VALUES (3, '{"player": {"name": "Henderson"}}');

SELECT '-- returns values in path argument order';
SELECT id, JSONValues(data, ['type.name', 'player.name']) FROM tab ORDER BY id;

SELECT '-- absent path omitted from row';
SELECT id, JSONValues(data, ['type.name', 'player.name']) FROM tab WHERE id = 3;

SELECT '-- single path';
SELECT id, JSONValues(data, ['player.name']) FROM tab ORDER BY id;

SELECT '-- path order is preserved (reversed args)';
SELECT id, JSONValues(data, ['player.name', 'type.name']) FROM tab WHERE id = 0;

SELECT '-- text index is used for subcolumn equality';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE data.`type.name` = 'goal'
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- non-constant path argument is rejected';
SELECT JSONValues(data, toString(id)) FROM tab; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE tab;

-- Regression: typed paths with non-nullable types cannot distinguish absent from
-- present-with-default (both store the type default, e.g. 0 for UInt32).
-- Both cases are omitted to avoid emitting phantom defaults for absent paths.
-- Declare the path as Nullable(T) to distinguish them if needed.
SELECT '-- typed path default equals absent: both omitted';
DROP TABLE IF EXISTS tab_typed;
CREATE TABLE tab_typed (id UInt32, json JSON(a UInt32)) ENGINE = Memory;
INSERT INTO tab_typed VALUES (1, '{"a": 0}'), (2, '{"a": 1}'), (3, '{}');
SELECT id, JSONValues(json, ['a']) FROM tab_typed ORDER BY id;
DROP TABLE tab_typed;

-- Safety guard: JSONValues omits typed-path default values from the index.
-- Without the guard, WHERE data.a = 0 would produce a false-negative skip:
-- the token "0" is absent from the granule's JSONValues index, so the index
-- would incorrectly report the granule as non-matching and skip it.
-- The guard detects that 0 is the UInt32 default and disables index skipping.
SELECT '-- safety guard: equality against typed-path default returns correct rows';
DROP TABLE IF EXISTS tab_guard;
CREATE TABLE tab_guard
(
    id UInt32,
    data JSON(a UInt32),
    INDEX idx JSONValues(data, ['a']) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab_guard VALUES (1, '{"a": 0}');
INSERT INTO tab_guard VALUES (2, '{"a": 1}');
INSERT INTO tab_guard VALUES (3, '{}');

-- Both a=0 and absent-a store the default 0, so both rows 1 and 3 must be returned.
SELECT id FROM tab_guard WHERE data.a = 0 ORDER BY id;
-- Non-default value: index filtering is effective (only row 2 matches).
SELECT id FROM tab_guard WHERE data.a = 1 ORDER BY id;

DROP TABLE tab_guard;
