-- `NULL IN (...)` under compare-nulls semantics (`transform_null_in = 1` or explicit `nullIn`)
-- is decided purely by `NULL` presence among the set elements - the constant `Set` path
-- does not require a common element type (`SELECT NULL IN (1, 'x', 2) SETTINGS transform_null_in = 1`
-- returns `0`). The row-wise rewrite for a non-constant RHS must do the same instead of
-- building an `array(...)` of the elements, which would fail with `NO_COMMON_TYPE` for a
-- heterogeneous RHS such as this one.

-- { echoOn }

-- Heterogeneous non-constant RHS with a runtime NULL element in row 0.
SELECT NULL IN (1, CAST(if(number = 0, NULL, 'x') AS Nullable(String)), CAST(number AS Nullable(UInt64))) FROM numbers(2) SETTINGS transform_null_in = 1, use_variant_as_common_type = 0, enable_analyzer = 0;
SELECT NULL IN (1, CAST(if(number = 0, NULL, 'x') AS Nullable(String)), CAST(number AS Nullable(UInt64))) FROM numbers(2) SETTINGS transform_null_in = 1, use_variant_as_common_type = 0, enable_analyzer = 1;

SELECT NULL NOT IN (1, CAST(if(number = 0, NULL, 'x') AS Nullable(String)), CAST(number AS Nullable(UInt64))) FROM numbers(2) SETTINGS transform_null_in = 1, use_variant_as_common_type = 0, enable_analyzer = 0;
SELECT NULL NOT IN (1, CAST(if(number = 0, NULL, 'x') AS Nullable(String)), CAST(number AS Nullable(UInt64))) FROM numbers(2) SETTINGS transform_null_in = 1, use_variant_as_common_type = 0, enable_analyzer = 1;

-- Explicit `nullIn` with a scalar runtime-null RHS.
SELECT nullIn(NULL, if(number = 0, NULL, 1)) FROM numbers(2) SETTINGS enable_analyzer = 0;
SELECT nullIn(NULL, if(number = 0, NULL, 1)) FROM numbers(2) SETTINGS enable_analyzer = 1;

-- Tuple-typed non-constant RHS: the set is the tuple's elements.
SELECT nullIn(NULL, materialize((1, NULL))), nullIn(NULL, materialize((1, 2))) SETTINGS enable_analyzer = 0;
SELECT nullIn(NULL, materialize((1, NULL))), nullIn(NULL, materialize((1, 2))) SETTINGS enable_analyzer = 1;

-- Array-typed non-constant RHS keeps the `has` rewrite, which already tests `NULL` presence.
SELECT nullIn(NULL, [materialize(1), NULL]), nullIn(NULL, [materialize(1), 2]) SETTINGS enable_analyzer = 0;
SELECT nullIn(NULL, [materialize(1), NULL]), nullIn(NULL, [materialize(1), 2]) SETTINGS enable_analyzer = 1;

-- The constant fast path for a literal NULL element still folds.
SELECT NULL IN (1, NULL, materialize(2)) SETTINGS transform_null_in = 1, enable_analyzer = 0;
SELECT NULL IN (1, NULL, materialize(2)) SETTINGS transform_null_in = 1, enable_analyzer = 1;
