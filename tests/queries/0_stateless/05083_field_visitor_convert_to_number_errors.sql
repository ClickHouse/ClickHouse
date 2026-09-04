-- FieldVisitorConvertToNumber (src/Common/FieldVisitorConvertToNumber.h) converts a Field
-- to a numeric type T when aggregate functions extract their parameters.  Several specialised
-- operator() overloads that handle non-numeric Field types had zero CI coverage because no
-- test ever passed a parameter of those types to an aggregate function constructor.
--
-- This test covers six previously-uncovered paths by passing wrong-typed and unusual-typed
-- literals to topK() at construction time:
--
--   src/Common/FieldVisitorConvertToNumber.h:26-28  -- Null    -> CANNOT_CONVERT_TYPE
--   src/Common/FieldVisitorConvertToNumber.h:31-33  -- String  -> CANNOT_CONVERT_TYPE
--   src/Common/FieldVisitorConvertToNumber.h:41-43  -- Tuple   -> CANNOT_CONVERT_TYPE
--   src/Common/FieldVisitorConvertToNumber.h:46-48  -- Map     -> CANNOT_CONVERT_TYPE
--   src/Common/FieldVisitorConvertToNumber.h:72     -- Float64 infinite value to integer -> CANNOT_CONVERT_TYPE
--   src/Common/FieldVisitorConvertToNumber.h:112    -- DecimalField<U> to integer (Decimal / scale) -> success

-- 1. Null parameter: operator()(const Null &) throws CANNOT_CONVERT_TYPE.
--    No aggregate function accepts NULL as a constructor parameter; passing it reaches
--    FieldVisitorConvertToNumber before any function-level type guard.
SELECT topK(NULL)(number) FROM numbers(3); -- { serverError CANNOT_CONVERT_TYPE }

-- 2. String parameter: operator()(const String &) throws CANNOT_CONVERT_TYPE.
SELECT topK('abc')(number) FROM numbers(3); -- { serverError CANNOT_CONVERT_TYPE }

-- 3. Tuple parameter: operator()(const Tuple &) throws CANNOT_CONVERT_TYPE.
SELECT topK((1, 2))(number) FROM numbers(3); -- { serverError CANNOT_CONVERT_TYPE }

-- 4. Map parameter: operator()(const Map &) throws CANNOT_CONVERT_TYPE.
SELECT topK(map('a', 1))(number) FROM numbers(3); -- { serverError CANNOT_CONVERT_TYPE }

-- 5. Infinite Float64 to integer: when T is not a floating-point type, the visitor
--    checks isFinite(x) first (line 64).  For non-bool T the else-branch at line 72
--    throws CANNOT_CONVERT_TYPE.  Both inf and nan are non-finite values.
SELECT topK(inf)(number) FROM numbers(3);  -- { serverError CANNOT_CONVERT_TYPE }
SELECT topK(nan)(number) FROM numbers(3);  -- { serverError CANNOT_CONVERT_TYPE }

-- 6. Decimal32 parameter to integer (line 112): the Decimal visitor branch
--    `if constexpr (!is_floating_point<T>)` executes
--    `(x.getValue() / x.getScaleMultiplier()).convertTo<T>()`.
--    With scale 0, this is a straightforward integer conversion.
--    topK(3) of {0..9} (all distinct) returns exactly 3 elements.
SELECT length(topK(3::Decimal32(0))(number)) = 3 AS ok FROM numbers(10);
