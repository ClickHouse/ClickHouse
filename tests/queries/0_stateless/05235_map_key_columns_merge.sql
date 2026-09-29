-- Tags: no-random-settings, no-random-merge-tree-settings

-- Merge semantics for the `with_key_columns` Map serialization: the merged part holds
-- the union of the source parts' key sets, and rows coming from a part lacking key k
-- keep presence = 0 for k after the merge.

DROP TABLE IF EXISTS t_merge_union;
CREATE TABLE t_merge_union
(
    id UInt32,
    m Map(String, UInt64)
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';

SYSTEM STOP MERGES t_merge_union;
-- Part 1: only key a. Part 2: keys b and c. Part 3: only empty maps.
INSERT INTO t_merge_union VALUES (1, {'a': 10}), (2, {'a': 20});
INSERT INTO t_merge_union VALUES (3, {'b': 30, 'c': 300}), (4, {'b': 40});
INSERT INTO t_merge_union VALUES (5, {}), (6, {});

SELECT 'parts before merge';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_union' AND active;

SYSTEM START MERGES t_merge_union;
OPTIMIZE TABLE t_merge_union FINAL;

SELECT 'parts after merge';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_union' AND active;

SELECT 'merged key set is the union';
SELECT groupUniqArrayArray(mapKeys(m)) FROM t_merge_union;

SELECT 'per-row presence and values after merge';
SELECT id, mapContains(m, 'a'), mapContains(m, 'b'), mapContains(m, 'c'),
       m['a'], m['b'], m['c']
FROM t_merge_union ORDER BY id;

SELECT id, m FROM t_merge_union ORDER BY id;
DROP TABLE t_merge_union;

-- Merge after ALTER ADD COLUMN: the old part has no m column at all, the table-level
-- DEFAULT evaluates per row when the parts are squashed together.
DROP TABLE IF EXISTS t_merge_default;
CREATE TABLE t_merge_default
(
    id UInt32,
    v UInt64
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';

SYSTEM STOP MERGES t_merge_default;
INSERT INTO t_merge_default VALUES (1, 100), (2, 200);

ALTER TABLE t_merge_default ADD COLUMN m Map(String, UInt64) DEFAULT map(concat('k', toString(id)), id);

-- Reading the old part must evaluate the DEFAULT.
SELECT id, m, mapKeys(m), mapContains(m, concat('k', toString(id))) FROM t_merge_default ORDER BY id;

INSERT INTO t_merge_default VALUES (3, 300, {'x': 5}), (4, 400, {'k4': 4});

SYSTEM START MERGES t_merge_default;
OPTIMIZE TABLE t_merge_default FINAL;

SELECT 'merged with default-derived keys';
SELECT id, m, mapContains(m, concat('k', toString(id))), mapContains(m, 'x') FROM t_merge_default ORDER BY id;
DROP TABLE t_merge_default;
