-- Regression test: a non-constant RHS of `IN` whose elements have no common supertype with the
-- LHS must not fail with `NO_COMMON_TYPE`. The rewrite falls back to casting each element to the
-- LHS type, mirroring the analyzer and the constant `Set` path (a failed `CAST` to a `Nullable`
-- target produces `NULL`, like the constant `Set` path skips unrepresentable elements).
-- The results are pinned to the pre-existing behavior of the default analyzer.

-- { echoOn }

SET enable_analyzer = 1;

SELECT 1 IN (materialize(1), 'x');
SELECT 3 IN (materialize(1), 'x');
SELECT 1 NOT IN (materialize(1), 'x');
SELECT 1 IN (materialize(1), 'x', NULL);
SELECT toNullable(1) IN (materialize(1), 'x');
SELECT number IN (materialize(1), 'x') FROM numbers(2);
-- Two rewrites of the same tuple in one query must not conflict in the actions DAG.
SELECT number IN (materialize(1), 'x'), number NOT IN (materialize(1), 'x') FROM numbers(2);
SELECT 1 IN (materialize('a'), 'b');
SELECT NULL IN (materialize(1), 'x');
SELECT NULL NOT IN (materialize(1), 'x');
SELECT number, if(number = 0, NULL, 1) IN (materialize(1), toDate('2020-01-02')) FROM numbers(2);
SELECT number, if(number = 0, NULL, 1) IN (materialize(1), 'x') FROM numbers(2);

SET transform_null_in = 1;

SELECT 1 IN (materialize(1), 'x'); -- { serverError CANNOT_PARSE_TEXT }
SELECT 1 IN (materialize(1), 'x', NULL);
SELECT toNullable(1) IN (materialize(1), 'x');
SELECT NULL IN (materialize(1), 'x');
SELECT NULL NOT IN (materialize(1), 'x');
SELECT number, if(number = 0, NULL, 1) IN (materialize(1), toDate('2020-01-02')) FROM numbers(2);
SELECT number, if(number = 0, NULL, 1) IN (materialize(1), 'x') FROM numbers(2);
