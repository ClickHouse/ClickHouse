-- Comparisons between a vector `String` and a constant `FixedString` used to follow a
-- plain `memcmp` path that respected the constant's trailing NUL padding, while the
-- vector-vs-vector path used zero-padded comparison (per the SQL semantics that
-- `toFixedString('abc', 5) = 'abc'`). That made the result depend on whether one of
-- the operands was held as a constant, breaking equality, distinctness, and ordering.

SET session_timezone = 'UTC';

-- equals across all const/materialize arrangements. All four should agree.
SELECT 'equals' AS op,
    equals(materialize(CAST('' AS String)), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS both_mat,
    equals(CAST('' AS String), CAST('\0\0\0\0' AS FixedString(4))) AS both_const,
    equals(materialize(CAST('' AS String)), CAST('\0\0\0\0' AS FixedString(4))) AS str_mat_fs_const,
    equals(CAST('' AS String), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS str_const_fs_mat,
    equals(materialize(CAST('\0\0\0\0' AS FixedString(4))), CAST('' AS String)) AS fs_mat_str_const,
    equals(CAST('\0\0\0\0' AS FixedString(4)), materialize(CAST('' AS String))) AS fs_const_str_mat;

SELECT 'notEquals' AS op,
    notEquals(materialize(CAST('' AS String)), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS both_mat,
    notEquals(CAST('' AS String), CAST('\0\0\0\0' AS FixedString(4))) AS both_const,
    notEquals(materialize(CAST('' AS String)), CAST('\0\0\0\0' AS FixedString(4))) AS str_mat_fs_const,
    notEquals(CAST('' AS String), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS str_const_fs_mat,
    notEquals(materialize(CAST('\0\0\0\0' AS FixedString(4))), CAST('' AS String)) AS fs_mat_str_const,
    notEquals(CAST('\0\0\0\0' AS FixedString(4)), materialize(CAST('' AS String))) AS fs_const_str_mat;

SELECT 'less' AS op,
    less(materialize(CAST('' AS String)), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS both_mat,
    less(CAST('' AS String), CAST('\0\0\0\0' AS FixedString(4))) AS both_const,
    less(materialize(CAST('' AS String)), CAST('\0\0\0\0' AS FixedString(4))) AS str_mat_fs_const,
    less(CAST('' AS String), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS str_const_fs_mat,
    less(materialize(CAST('\0\0\0\0' AS FixedString(4))), CAST('' AS String)) AS fs_mat_str_const,
    less(CAST('\0\0\0\0' AS FixedString(4)), materialize(CAST('' AS String))) AS fs_const_str_mat;

SELECT 'lessOrEquals' AS op,
    lessOrEquals(materialize(CAST('' AS String)), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS both_mat,
    lessOrEquals(CAST('' AS String), CAST('\0\0\0\0' AS FixedString(4))) AS both_const,
    lessOrEquals(materialize(CAST('' AS String)), CAST('\0\0\0\0' AS FixedString(4))) AS str_mat_fs_const,
    lessOrEquals(CAST('' AS String), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS str_const_fs_mat,
    lessOrEquals(materialize(CAST('\0\0\0\0' AS FixedString(4))), CAST('' AS String)) AS fs_mat_str_const,
    lessOrEquals(CAST('\0\0\0\0' AS FixedString(4)), materialize(CAST('' AS String))) AS fs_const_str_mat;

SELECT 'isNotDistinctFrom' AS op,
    isNotDistinctFrom(materialize(CAST('' AS String)), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS both_mat,
    isNotDistinctFrom(CAST('' AS String), CAST('\0\0\0\0' AS FixedString(4))) AS both_const,
    isNotDistinctFrom(materialize(CAST('' AS String)), CAST('\0\0\0\0' AS FixedString(4))) AS str_mat_fs_const,
    isNotDistinctFrom(CAST('' AS String), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS str_const_fs_mat,
    isNotDistinctFrom(materialize(CAST('\0\0\0\0' AS FixedString(4))), CAST('' AS String)) AS fs_mat_str_const,
    isNotDistinctFrom(CAST('\0\0\0\0' AS FixedString(4)), materialize(CAST('' AS String))) AS fs_const_str_mat;

SELECT 'isDistinctFrom' AS op,
    isDistinctFrom(materialize(CAST('' AS String)), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS both_mat,
    isDistinctFrom(CAST('' AS String), CAST('\0\0\0\0' AS FixedString(4))) AS both_const,
    isDistinctFrom(materialize(CAST('' AS String)), CAST('\0\0\0\0' AS FixedString(4))) AS str_mat_fs_const,
    isDistinctFrom(CAST('' AS String), materialize(CAST('\0\0\0\0' AS FixedString(4)))) AS str_const_fs_mat,
    isDistinctFrom(materialize(CAST('\0\0\0\0' AS FixedString(4))), CAST('' AS String)) AS fs_mat_str_const,
    isDistinctFrom(CAST('\0\0\0\0' AS FixedString(4)), materialize(CAST('' AS String))) AS fs_const_str_mat;

-- Non-empty `String` vs `FixedString` with trailing padding: `'abc'::String == 'abc\0\0'::FixedString(5)`.
SELECT 'abc/abc_padded' AS shape,
    equals(materialize(CAST('abc' AS String)), CAST(toFixedString('abc', 5) AS FixedString(5))) AS str_mat_fs_const,
    equals(materialize(CAST('abc' AS String)), materialize(CAST(toFixedString('abc', 5) AS FixedString(5)))) AS both_mat,
    less(materialize(CAST('abc' AS String)), CAST(toFixedString('abc', 5) AS FixedString(5))) AS less_str_mat_fs_const,
    less(materialize(CAST('abc' AS String)), materialize(CAST(toFixedString('abc', 5) AS FixedString(5)))) AS less_both_mat;

-- `String` with embedded data vs `FixedString` with the same data plus padding.
-- Use a longer `FixedString` to ensure the constant trailing NULs do not bias the order.
SELECT 'abc/abc_padded_124' AS shape,
    equals(materialize(CAST('abc' AS String)), CAST(toFixedString('abc', 124) AS FixedString(124))) AS str_mat_fs_const,
    equals(CAST('abc' AS String), CAST(toFixedString('abc', 124) AS FixedString(124))) AS both_const;

-- A regression for the original report: trailing NUL padding must not affect `less`
-- when one side is a materialized empty `String`.
SELECT less(materialize(CAST('' AS String)), CAST(toFixedString('', 124) AS FixedString(124))) AS less_empty_vs_fs124;
