-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS arrayfold_member_04839;

-- arrayFold((acc, x) -> tupleElement(x, 'doc'), arr, seed): the body reads only x.doc, so the donor is
-- `arr.doc` and not the whole `arr`. Donating the array itself would offer a tuple source for a bare
-- JSON target, which mergeJSONSharedDataPathRules refuses, dropping doc's retained rule entirely.
CREATE TABLE arrayfold_member_04839
(
    id UInt64,
    arr Array(Tuple(doc JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'), other JSON(max_dynamic_paths=5, SHARED REGEXP '^oth_'))),
    -- arrayFold requires the lambda's return type to equal the accumulator's, and `x.doc` keeps its
    -- retained rule, so the seed has to carry the same one.
    seed JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO arrayfold_member_04839 VALUES (1, [('{"tag_a1":1}', '{"oth_1":2}')], '{}');

-- Drop the rules from the column types, so the projection's own retained policy is what is tested.
ALTER TABLE arrayfold_member_04839
    MODIFY COLUMN arr Array(Tuple(doc JSON(max_dynamic_paths=5), other JSON(max_dynamic_paths=5)));
ALTER TABLE arrayfold_member_04839 MODIFY COLUMN seed JSON(max_dynamic_paths=5);

ALTER TABLE arrayfold_member_04839
    ADD PROJECTION p (SELECT id, arrayFold((acc, x) -> tupleElement(x, 'doc'), arr, seed) WHERE id > 0 ORDER BY id);
ALTER TABLE arrayfold_member_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- Print the type instead of counting one rule present and its sibling absent. Note what no oracle
-- can prove here: arrayFold requires the accumulator's type to equal the lambda's return type, so
-- `seed` has to carry `^tag_` as well, and it keeps donating that rule even if the `arr.doc` donor
-- regresses to the whole `arr`. The member-qualified donor is pinned discriminatingly by the
-- scalar-member case in 04839_json_shared_regexp_projection_arraymap_tuple.sql, which has no seed.
SELECT
    'arrayFold donates the member the body reads',
    type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='arrayfold_member_04839' AND column != 'id' AND active;

DROP TABLE arrayfold_member_04839;
