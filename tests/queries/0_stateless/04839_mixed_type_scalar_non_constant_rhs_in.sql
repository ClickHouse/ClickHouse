-- A single scalar non-constant RHS of IN is a one-element set. When the LHS and the RHS have no
-- common supertype, the row-wise comparison falls back to casting the RHS to the LHS type, the
-- same as the tuple/array rewrite - a failed CAST to a Nullable target produces NULL, like the
-- constant Set path skipping unrepresentable elements. Both analyzers must agree.

-- { echoOn }

SET enable_analyzer = 1;

SELECT 1 IN (materialize('1'));
SELECT 1 IN (materialize('x'));
SELECT 1 NOT IN (materialize('1'));
SELECT 1 NOT IN (materialize('x'));
SELECT 1 IN (materialize('1')) SETTINGS transform_null_in = 1;
SELECT 1 IN (materialize('x')) SETTINGS transform_null_in = 1; -- { serverError CANNOT_PARSE_TEXT }
SELECT nullIn(1, materialize('1'));
SELECT nullIn(1, materialize('x')); -- { serverError CANNOT_PARSE_TEXT }
SELECT toDate('2020-01-01') IN (materialize('2020-01-01'));
SELECT toDate('2020-01-01') NOT IN (materialize('2020-01-02'));
SELECT toNullable(1) IN (materialize('1'));
SELECT NULL IN (materialize('1'));
-- a common supertype exists: compared in the supertype, as before
SELECT toUInt8(1) IN (materialize(1000));
SELECT 1 IN (materialize(NULL));

SET enable_analyzer = 0;

SELECT 1 IN (materialize('1'));
SELECT 1 IN (materialize('x'));
SELECT 1 NOT IN (materialize('1'));
SELECT 1 NOT IN (materialize('x'));
SELECT 1 IN (materialize('1')) SETTINGS transform_null_in = 1;
SELECT 1 IN (materialize('x')) SETTINGS transform_null_in = 1; -- { serverError CANNOT_PARSE_TEXT }
SELECT nullIn(1, materialize('1'));
SELECT nullIn(1, materialize('x')); -- { serverError CANNOT_PARSE_TEXT }
SELECT toDate('2020-01-01') IN (materialize('2020-01-01'));
SELECT toDate('2020-01-01') NOT IN (materialize('2020-01-02'));
SELECT toNullable(1) IN (materialize('1'));
SELECT NULL IN (materialize('1'));
SELECT toUInt8(1) IN (materialize(1000));
SELECT 1 IN (materialize(NULL));
