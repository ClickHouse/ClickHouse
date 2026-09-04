-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS arrayfold_seed_04839;

-- arrayFold(lambda(tuple(acc, x), body), arr, seed): seed can itself become the result (e.g. an
-- empty array), so it must always donate regardless of whether x (bound to arr) is used in the body.
CREATE TABLE arrayfold_seed_04839
(
    id UInt64,
    arr Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a')),
    seed JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO arrayfold_seed_04839 VALUES (1, ['{"tag_a1":1}'], '{"tag_b1":2}');

ALTER TABLE arrayfold_seed_04839 MODIFY COLUMN arr Array(JSON(max_dynamic_paths=5));
ALTER TABLE arrayfold_seed_04839 MODIFY COLUMN seed JSON(max_dynamic_paths=5);

ALTER TABLE arrayfold_seed_04839
    ADD PROJECTION p (SELECT id, arrayFold((acc, x) -> acc, arr, seed) WHERE id > 0 ORDER BY id);
ALTER TABLE arrayfold_seed_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- The regression: x is unused so arr's rule (tag_a) must not appear; seed's rule (tag_b) always must.
SELECT
    'arrayFold seed always donates, unused array does not',
    countIf(position(type, '^tag_b') > 0 AND position(type, '^tag_a') = 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='arrayfold_seed_04839' AND column != 'id' AND active;

DROP TABLE arrayfold_seed_04839;
