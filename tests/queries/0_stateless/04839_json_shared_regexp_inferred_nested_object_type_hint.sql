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
    -- The third rule carries an escaped quote beside dots on purpose. The expanded variant name then
    -- holds a dot *inside* a string literal, both in the rule and in the generated
    -- `shared_regexp_path_prefix='three.'`, which is what makes splitSubcolumnName's escaped-literal
    -- handling load-bearing: it must not split the subcolumn off at one of those dots.
    j JSON(max_dynamic_paths=10, SHARED REGEXP '^arr[.]forced$', SHARED REGEXP '^two[.]flag$', SHARED REGEXP '^three[.]a\'b$')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO inferred_nested_hint_04839 VALUES (1, '{"arr":[{"forced":1,"keep":2},{"forced":3,"keep":4}],"two":[{"flag":"a","keep":5},{"flag":"b","keep":6}],"three":[{"a\'b":9,"keep":7}]}');

-- The regression: both reads come back empty/NULL when the shorthand misses the prefixed variant.
-- "forced" is the shared-data path here, "keep" the dynamic one, so both placements are covered.
SELECT
    'Array(JSON) shorthand reaches the prefixed nested variant',
    j.arr.:`Array(JSON)`.forced,
    j.arr.:`Array(JSON)`.keep,
    j.two.:`Array(JSON)`.flag,
    j.two.:`Array(JSON)`.keep,
    j.three.:`Array(JSON)`.`a'b`,
    j.three.:`Array(JSON)`.keep
FROM inferred_nested_hint_04839;

-- Must survive a metadata reload, not just the freshly-parsed in-memory column.
DETACH TABLE inferred_nested_hint_04839;
ATTACH TABLE inferred_nested_hint_04839;

SELECT
    'Array(JSON) shorthand after reload',
    j.arr.:`Array(JSON)`.forced,
    j.arr.:`Array(JSON)`.keep,
    j.two.:`Array(JSON)`.flag,
    j.two.:`Array(JSON)`.keep,
    j.three.:`Array(JSON)`.`a'b`,
    j.three.:`Array(JSON)`.keep
FROM inferred_nested_hint_04839;

DROP TABLE inferred_nested_hint_04839;
