-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;
SET mutations_sync = 2;

-- Inferred nested objects never carry SHARED REGEXP in their type, so removing the rule and
-- widening the type later must leave the same nested type names as a table that never had the rule.
DROP TABLE IF EXISTS retired_rule_04839;
DROP TABLE IF EXISTS control_04839;

CREATE TABLE retired_rule_04839 (id UInt64, j JSON(max_dynamic_paths=10, max_dynamic_types=2, SHARED REGEXP '^tag_'))
ENGINE = MergeTree ORDER BY id;
CREATE TABLE control_04839 (id UInt64, j JSON(max_dynamic_paths=10, max_dynamic_types=2))
ENGINE = MergeTree ORDER BY id;

-- Two frequent scalar types fill max_dynamic_types, so the rare nested object goes to the shared variant.
INSERT INTO retired_rule_04839 SELECT number, concat('{"arr": ', toString(number), '}') FROM numbers(10);
INSERT INTO retired_rule_04839 SELECT 100 + number, concat('{"arr": "s', toString(number), '"}') FROM numbers(10);
INSERT INTO retired_rule_04839 VALUES (999, '{"arr": [{"tag_x": 1}]}');
OPTIMIZE TABLE retired_rule_04839 FINAL;

INSERT INTO control_04839 SELECT number, concat('{"arr": ', toString(number), '}') FROM numbers(10);
INSERT INTO control_04839 SELECT 100 + number, concat('{"arr": "s', toString(number), '"}') FROM numbers(10);
INSERT INTO control_04839 VALUES (999, '{"arr": [{"tag_x": 1}]}');
OPTIMIZE TABLE control_04839 FINAL;

SELECT 'rule', id, dynamicType(j.arr), isDynamicElementInSharedData(j.arr), dynamicElement(j.arr, 'Array(JSON(max_dynamic_types=1, max_dynamic_paths=2))')
FROM retired_rule_04839 WHERE id = 999;
SELECT 'control', id, dynamicType(j.arr), isDynamicElementInSharedData(j.arr), dynamicElement(j.arr, 'Array(JSON(max_dynamic_types=1, max_dynamic_paths=2))')
FROM control_04839 WHERE id = 999;

-- Remove the rule, then write a row after the removal.
ALTER TABLE retired_rule_04839 MODIFY COLUMN j JSON(max_dynamic_paths=10, max_dynamic_types=2);
INSERT INTO retired_rule_04839 VALUES (2000, '{"arr": [{"tag_y": 2}]}');
OPTIMIZE TABLE retired_rule_04839 FINAL;

INSERT INTO control_04839 VALUES (2000, '{"arr": [{"tag_y": 2}]}');
OPTIMIZE TABLE control_04839 FINAL;

SELECT 'after removal', 'rule', id, dynamicType(j.arr), isDynamicElementInSharedData(j.arr), dynamicElement(j.arr, 'Array(JSON(max_dynamic_types=1, max_dynamic_paths=2))')
FROM retired_rule_04839 WHERE id IN (999, 2000) ORDER BY id;
SELECT 'after removal', 'control', id, dynamicType(j.arr), isDynamicElementInSharedData(j.arr), dynamicElement(j.arr, 'Array(JSON(max_dynamic_types=1, max_dynamic_paths=2))')
FROM control_04839 WHERE id IN (999, 2000) ORDER BY id;

-- An unrelated widening frees a max_dynamic_types slot, so the nested object leaves the shared variant.
ALTER TABLE retired_rule_04839 MODIFY COLUMN j JSON(max_dynamic_paths=10, max_dynamic_types=3);
ALTER TABLE control_04839 MODIFY COLUMN j JSON(max_dynamic_paths=10, max_dynamic_types=3);

SELECT 'after widening', 'rule', id, dynamicType(j.arr), isDynamicElementInSharedData(j.arr), dynamicElement(j.arr, 'Array(JSON(max_dynamic_types=1, max_dynamic_paths=2))')
FROM retired_rule_04839 WHERE id IN (999, 2000) ORDER BY id;
SELECT 'after widening', 'control', id, dynamicType(j.arr), isDynamicElementInSharedData(j.arr), dynamicElement(j.arr, 'Array(JSON(max_dynamic_types=1, max_dynamic_paths=2))')
FROM control_04839 WHERE id IN (999, 2000) ORDER BY id;

SELECT 'distinct nested types', 'rule', arraySort(groupUniqArray(dynamicType(j.arr))) FROM retired_rule_04839;
SELECT 'distinct nested types', 'control', arraySort(groupUniqArray(dynamicType(j.arr))) FROM control_04839;

DROP TABLE retired_rule_04839;
DROP TABLE control_04839;
