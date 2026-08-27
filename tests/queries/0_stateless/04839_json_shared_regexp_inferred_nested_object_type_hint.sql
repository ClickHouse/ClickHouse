-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS inferred_nested_hint_04839;

-- The documented `.:`Array(JSON)`` shorthand expands to the inferred nested-object type. Once that
-- type carries this path's own rules and prefix, the pathless expansion no longer names the variant.
-- Two arrays with independent rules pin that each hint expands against *its own* path prefix, not a
-- sibling's: the patterns are carried whole, only `shared_regexp_path_prefix` differs per path.
CREATE TABLE inferred_nested_hint_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=10, SHARED REGEXP '^arr[.]forced$', SHARED REGEXP '^two[.]flag$')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO inferred_nested_hint_04839 VALUES (1, '{"arr":[{"forced":1,"keep":2},{"forced":3,"keep":4}],"two":[{"flag":"a","keep":5},{"flag":"b","keep":6}]}');

-- The regression: both reads come back empty/NULL when the shorthand misses the prefixed variant.
-- "forced" is the shared-data path here, "keep" the dynamic one, so both placements are covered.
SELECT
    'Array(JSON) shorthand reaches the prefixed nested variant',
    j.arr.:`Array(JSON)`.forced,
    j.arr.:`Array(JSON)`.keep,
    j.two.:`Array(JSON)`.flag,
    j.two.:`Array(JSON)`.keep
FROM inferred_nested_hint_04839;

-- Must survive a metadata reload, not just the freshly-parsed in-memory column.
DETACH TABLE inferred_nested_hint_04839;
ATTACH TABLE inferred_nested_hint_04839;

SELECT
    'Array(JSON) shorthand after reload',
    j.arr.:`Array(JSON)`.forced,
    j.arr.:`Array(JSON)`.keep,
    j.two.:`Array(JSON)`.flag,
    j.two.:`Array(JSON)`.keep
FROM inferred_nested_hint_04839;

DROP TABLE inferred_nested_hint_04839;
