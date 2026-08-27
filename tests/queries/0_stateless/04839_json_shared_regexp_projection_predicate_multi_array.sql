-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS predicate_multiarray_04839;

-- The predicate-only higher-order functions return elements of their *first* collection only, so the
-- trailing condition/sort-key collections are not value donors even though they are trailing arguments.
CREATE TABLE predicate_multiarray_04839
(
    id UInt64,
    plain Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a')),
    pol Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO predicate_multiarray_04839 VALUES (1, ['{"tag_a1":1}'], ['{"flag":1,"tag_b1":2}']);

-- Retire both rules from the current metadata first, like the sibling projection tests: otherwise the
-- projection column types carry them straight from the projection metadata and prove nothing.
ALTER TABLE predicate_multiarray_04839
    MODIFY COLUMN plain Array(JSON(max_dynamic_paths=5)),
    MODIFY COLUMN pol Array(JSON(max_dynamic_paths=5));

ALTER TABLE predicate_multiarray_04839
    ADD PROJECTION p (SELECT id, arrayFilter((x, y) -> JSONHas(y, 'flag'), plain, pol) WHERE id > 0 ORDER BY id);
-- arrayPartialSort/arrayTopK put the output-carrying collection one argument later, after the limit.
ALTER TABLE predicate_multiarray_04839
    ADD PROJECTION q (SELECT id, arrayPartialSort((x, y) -> toString(y), 1, plain, pol) WHERE id > 0 ORDER BY id);
ALTER TABLE predicate_multiarray_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;
ALTER TABLE predicate_multiarray_04839 MATERIALIZE PROJECTION q SETTINGS mutations_sync=1;

-- The regression: only tag_a (plain's rule) may appear; tag_b (pol's, read only by the predicate) must not.
SELECT
    'arrayFilter donates only the filtered collection',
    countIf(position(type, '^tag_a') > 0 AND position(type, '^tag_b') = 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='predicate_multiarray_04839' AND part_name='p' AND column != 'id' AND active;

SELECT
    'arrayPartialSort donates only the sorted collection',
    countIf(position(type, '^tag_a') > 0 AND position(type, '^tag_b') = 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='predicate_multiarray_04839' AND part_name='q' AND column != 'id' AND active;

DROP TABLE predicate_multiarray_04839;
