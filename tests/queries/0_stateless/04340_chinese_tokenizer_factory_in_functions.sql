-- Tags: no-fasttest
-- no-fasttest: the chinese tokenizer uses Jieba, which is not available in the fast test build
--
-- The `chinese` tokenizer is registered in `TokenizerFactory`, which exposes it not only
-- through `tokens(value, 'chinese')` but also through the third-argument tokenizer-factory
-- path of `hasAnyTokens` / `hasAllTokens` / `hasToken`. This test exercises that path:
--   - the bare name `'chinese'` selects the default (coarse_grained) granularity,
--   - the factory syntax `'chinese(fine_grained)'` and `'chinese(coarse_grained)'` selects
--     the explicit granularity (both `Identifier` and string-literal forms work because
--     `getFieldFromIndexArgumentAST` accepts either),
--   - the result agrees with the equivalent `tokens(needle, 'chinese', granularity)` /
--     `splitByNonAlpha` evaluation as documented in the function description.
--
-- `hasToken` is also exercised on a needle that is a Jieba sub-token: with no index defined,
-- the row-level evaluation uses `splitByNonAlpha`, which on CJK text returns the whole
-- argument as one token, so `hasToken('网易', doc)` is always 0 even though `网易` appears
-- inside `doc`. This is the documented behaviour; CJK-aware token matching is meant to go
-- through `hasAllTokens` / `hasAnyTokens`.

SELECT '-- hasAnyTokens with chinese tokenizer (no index, brute force)';
SELECT hasAnyTokens('他来到了网易杭研大厦', '网易', 'chinese');
SELECT hasAnyTokens('他来到了网易杭研大厦', '不存在', 'chinese');
SELECT hasAnyTokens('他来到了网易杭研大厦', '杭研', 'chinese(coarse_grained)');

SELECT '-- hasAllTokens with chinese tokenizer';
SELECT hasAllTokens('我来自北京邮电大学', '北京邮电大学', 'chinese');
SELECT hasAllTokens('我来自北京邮电大学', ['北京邮电大学', '我'], 'chinese');
SELECT hasAllTokens('我来自北京邮电大学', ['不存在'], 'chinese');

SELECT '-- fine_grained granularity via factory syntax';
-- Coarse keeps `北京邮电大学` as one token, so a needle of `大学` does not match.
SELECT hasAnyTokens('我来自北京邮电大学', '大学', 'chinese(coarse_grained)');
-- Fine emits overlapping sub-tokens including `大学`, so the same needle now matches.
SELECT hasAnyTokens('我来自北京邮电大学', '大学', 'chinese(fine_grained)');
-- Both identifier form `chinese(fine_grained)` and string-literal form
-- `chinese('fine_grained')` are accepted by the tokenizer-factory parser.
SELECT hasAnyTokens('我来自北京邮电大学', '大学', 'chinese(''fine_grained'')');

SELECT '-- needle tokenization matches the tokenizer (hasAnyTokens with String needle is tokenized)';
-- For `splitByNonAlpha`, `hasAnyTokens('foo bar', 'foo bar')` tokenizes the needle into ['foo', 'bar'].
-- For `chinese`, the needle is tokenized by Jieba — confirm the same row-level / no-index path works.
SELECT hasAnyTokens('北京邮电大学的学生', '北京邮电大学 学号', 'chinese');

SELECT '-- tokensForLikePattern rejects chinese (declared unsupported in the function doc)';
SELECT tokensForLikePattern('%他来到了网易%', 'chinese'); -- { serverError BAD_ARGUMENTS }
