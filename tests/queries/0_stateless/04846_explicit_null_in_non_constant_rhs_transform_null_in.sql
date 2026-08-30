-- The explicit nullIn / notNullIn functions compare NULLs by definition, so the row-wise rewrite
-- of a non-constant RHS must not change its behavior with the transform_null_in setting - the
-- setting only renames in to nullIn before the rewrite. A mixed-type RHS with no common supertype
-- is cast to the LHS type: unrepresentable values throw, like the constant Set path, under both
-- values of the setting. A NULL value in a Nullable RHS is not a cast failure and compares as
-- NULL: it matches a NULL LHS and does not match a non-NULL LHS.

-- { echoOn }

SET enable_analyzer = 1;

SET transform_null_in = 1;
SELECT nullIn(1, materialize('x')); -- { serverError CANNOT_PARSE_TEXT }
SELECT notNullIn(1, materialize('x')); -- { serverError CANNOT_PARSE_TEXT }
SELECT nullIn(1, (materialize('x'), materialize('y'))); -- { serverError CANNOT_PARSE_TEXT }
SELECT nullIn(1, materialize('1'));
SELECT notNullIn(1, materialize('1'));
SELECT nullIn(1, materialize(CAST(NULL, 'Nullable(String)')));
SELECT notNullIn(1, materialize(CAST(NULL, 'Nullable(String)')));
SELECT nullIn(CAST(materialize(NULL), 'Nullable(UInt8)'), materialize(CAST(NULL, 'Nullable(String)')));
SELECT nullIn(toNullable(1), materialize(CAST(NULL, 'Nullable(String)')));

SET transform_null_in = 0;
SELECT nullIn(1, materialize('x')); -- { serverError CANNOT_PARSE_TEXT }
SELECT notNullIn(1, materialize('x')); -- { serverError CANNOT_PARSE_TEXT }
SELECT nullIn(1, (materialize('x'), materialize('y'))); -- { serverError CANNOT_PARSE_TEXT }
SELECT nullIn(1, materialize('1'));
SELECT notNullIn(1, materialize('1'));
SELECT nullIn(1, materialize(CAST(NULL, 'Nullable(String)')));
SELECT notNullIn(1, materialize(CAST(NULL, 'Nullable(String)')));
SELECT nullIn(CAST(materialize(NULL), 'Nullable(UInt8)'), materialize(CAST(NULL, 'Nullable(String)')));
SELECT nullIn(toNullable(1), materialize(CAST(NULL, 'Nullable(String)')));
