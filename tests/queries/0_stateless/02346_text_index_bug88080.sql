-- Test for issue #88080

SET allow_experimental_full_text_index = 1;

SELECT hasAllTokens('a', '[[(2,1)]]'::Polygon); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
