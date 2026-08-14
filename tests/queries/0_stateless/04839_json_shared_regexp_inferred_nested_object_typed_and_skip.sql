-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS inferred_nested_typed_skip_04839;

-- DataTypeObject::getTypeOfNestedObjects(path_prefix) used to carry the SHARED REGEXP rules and
-- prefix into an inferred nested object's own type (see 04839_json_shared_regexp_inferred_nested_object)
-- but drop the parent's typed paths and literal SKIP paths declared under that same prefix. That
-- inverted the documented precedence of typed paths / SKIP over SHARED REGEXP for inferred nested
-- objects specifically: `arr.forced`, despite being declared a typed UInt64 path, would be treated
-- as shared/dynamic inside an inferred element, and `arr.skip`, despite SKIP, would not be discarded.
-- As with the plain nested-object case, this only matters for inconsistent-shape array elements --
-- direct nesting gets flattened during the initial parse and never reaches this inference path.
--
-- Selecting a nested typed subcolumn (`j.arr[1].forced`) by itself, with nothing else touching `j`,
-- resolves to NULL regardless of this fix -- a separate, pre-existing gap in subcolumn resolution
-- for a typed path nested under a *dynamically inferred* (not declared) array, seemingly because
-- that path pairs the query analyzer's declared-schema-based resolution against a path that only
-- exists in the part's own inferred-at-insert-time type. Not something this fix touches or claims to
-- cover; JSONAllPathsWithTypes is used below instead, which reliably reflects the real stored type.
CREATE TABLE inferred_nested_typed_skip_04839
(
    id UInt64,
    j JSON(`arr.forced` UInt64, SKIP arr.skip, SHARED REGEXP '^arr[.]forced$')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO inferred_nested_typed_skip_04839 VALUES
    (1, '{"arr":[{"forced":1,"skip":2,"keep":3},{"forced":4,"skip":5,"other":6}]}');

-- The regression: without projecting the typed path and SKIP, `forced` would show as a dynamic type
-- (not the declared UInt64) and `skip` would still appear -- in shared data or as a dynamic path --
-- inside each inferred element instead of being discarded entirely.
SELECT
    'typed and skipped paths inside inferred nested elements',
    JSONAllPathsWithTypes(j.arr[1]),
    JSONAllPathsWithTypes(j.arr[2]),
    j.arr[1].forced,
    j.arr[2].forced
FROM inferred_nested_typed_skip_04839;

-- Must survive a metadata reload, not just exist in the freshly-parsed in-memory column.
DETACH TABLE inferred_nested_typed_skip_04839;
ATTACH TABLE inferred_nested_typed_skip_04839;

SELECT
    'typed and skipped paths inside inferred nested elements after reload',
    JSONAllPathsWithTypes(j.arr[1]),
    JSONAllPathsWithTypes(j.arr[2]),
    j.arr[1].forced,
    j.arr[2].forced
FROM inferred_nested_typed_skip_04839;

DROP TABLE inferred_nested_typed_skip_04839;
