-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS retired_matcher_shared_variant_04839;

-- A nested object that sits in a dynamic path's shared variant carries the type name it was
-- written under, in the header ahead of its bytes. Rewriting only the shared-variant statistics
-- leaves that name behind, and it does not stay put: it holds a max_dynamic_types slot, so the
-- next widening ALTER promotes it out and the retired name absorbs values written after the
-- policy was retired. The column then has no row on the type it declares, and a read naming the
-- declared type comes back empty rather than failing.
CREATE TABLE retired_matcher_shared_variant_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=10, max_dynamic_types=2, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id;

-- Two frequent scalar types fill max_dynamic_types, so the rare nested object spills.
INSERT INTO retired_matcher_shared_variant_04839 SELECT number, concat('{"arr": ', toString(number), '}') FROM numbers(10);
INSERT INTO retired_matcher_shared_variant_04839 SELECT 100 + number, concat('{"arr": "s', toString(number), '"}') FROM numbers(10);
INSERT INTO retired_matcher_shared_variant_04839 VALUES (999, '{"arr": [{"tag_x": 1}]}');
OPTIMIZE TABLE retired_matcher_shared_variant_04839 FINAL;

ALTER TABLE retired_matcher_shared_variant_04839 MODIFY COLUMN j JSON(max_dynamic_paths=10, max_dynamic_types=2);
INSERT INTO retired_matcher_shared_variant_04839 VALUES (2000, '{"arr": [{"tag_y": 2}]}');
OPTIMIZE TABLE retired_matcher_shared_variant_04839 FINAL;

SELECT 'after retirement', id, position(dynamicType(j.arr), 'tag_') = 0 AS matcher_gone
FROM retired_matcher_shared_variant_04839 WHERE id IN (999, 2000) ORDER BY id;

-- An unrelated widening. This is what promotes the shared-variant value back out, and what used
-- to spread the retired name onto the row inserted after the retirement.
ALTER TABLE retired_matcher_shared_variant_04839 MODIFY COLUMN j JSON(max_dynamic_paths=10, max_dynamic_types=3);

SELECT 'after widening', id, position(dynamicType(j.arr), 'tag_') = 0 AS matcher_gone
FROM retired_matcher_shared_variant_04839 WHERE id IN (999, 2000) ORDER BY id;

-- The values themselves are never in doubt; it is the name that decides whether they can be read
-- by the type the table declares.
SELECT 'values intact', id, j.arr
FROM retired_matcher_shared_variant_04839 WHERE id IN (999, 2000) ORDER BY id;

DROP TABLE retired_matcher_shared_variant_04839;
