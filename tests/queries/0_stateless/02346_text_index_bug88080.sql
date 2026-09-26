-- Test for issue #88080

SELECT hasAllTokens('a', '[[(2,1)]]'::Polygon); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
