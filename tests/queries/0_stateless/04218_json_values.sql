-- Tests for JSONValues function

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    data JSON(max_dynamic_paths=2),
    INDEX idx JSONValues(data, 'type.name', 'player.name') TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (0, '{"type": {"name": "goal"}, "player": {"name": "Salah"}, "score": 1}');
INSERT INTO tab VALUES (1, '{"type": {"name": "assist"}, "player": {"name": "Trent"}, "score": 0}');
INSERT INTO tab VALUES (2, '{"type": {"name": "goal"}, "player": {"name": "Firmino"}, "score": 2}');
INSERT INTO tab VALUES (3, '{"player": {"name": "Henderson"}}');

SELECT '-- returns values in path argument order';
SELECT id, JSONValues(data, 'type.name', 'player.name') FROM tab ORDER BY id;

SELECT '-- absent path omitted from row';
SELECT id, JSONValues(data, 'type.name', 'player.name') FROM tab WHERE id = 3;

SELECT '-- single path';
SELECT id, JSONValues(data, 'player.name') FROM tab ORDER BY id;

SELECT '-- path order is preserved (reversed args)';
SELECT id, JSONValues(data, 'player.name', 'type.name') FROM tab WHERE id = 0;

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
