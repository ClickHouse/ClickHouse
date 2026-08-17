-- Tags: no-fasttest
-- no-fasttest: the chinese tokenizer uses Jieba, which is not available in the fast test build
--
-- `chinese` is registered in `TokenizerFactory`, so besides `tokens(value, 'chinese')` it is
-- also accepted as the tokenizer argument of `hasAnyTokens` / `hasAllTokens`. The granularity
-- can be passed with the factory syntax `chinese(coarse_grained)` / `chinese(fine_grained)`,
-- in either identifier or string-literal form.

SELECT '-- hasAnyTokens with chinese tokenizer (no index, brute force)';
SELECT hasAnyTokens('他来到了网易杭研大厦', '网易', 'chinese');
SELECT hasAnyTokens('他来到了网易杭研大厦', '不存在', 'chinese');
SELECT hasAnyTokens('他来到了网易杭研大厦', '杭研', 'chinese(coarse_grained)');

SELECT '-- hasAllTokens with chinese tokenizer';
SELECT hasAllTokens('我来自北京邮电大学', '北京邮电大学', 'chinese');
SELECT hasAllTokens('我来自北京邮电大学', ['北京邮电大学', '我'], 'chinese');
SELECT hasAllTokens('我来自北京邮电大学', ['不存在'], 'chinese');

SELECT '-- granularity via factory syntax';
-- Coarse keeps `北京邮电大学` as one token, so a needle of `大学` does not match;
-- fine emits overlapping sub-tokens including `大学`, so it matches.
SELECT hasAnyTokens('我来自北京邮电大学', '大学', 'chinese(coarse_grained)');
SELECT hasAnyTokens('我来自北京邮电大学', '大学', 'chinese(fine_grained)');
-- Both the identifier form `chinese(fine_grained)` and the string-literal form
-- `chinese('fine_grained')` are accepted.
SELECT hasAnyTokens('我来自北京邮电大学', '大学', 'chinese(''fine_grained'')');

SELECT '-- a String needle is tokenized with the same tokenizer';
SELECT hasAnyTokens('北京邮电大学的学生', '北京邮电大学 学号', 'chinese');

SELECT '-- tokensForLikePattern rejects chinese (declared unsupported in the function doc)';
SELECT tokensForLikePattern('%他来到了网易%', 'chinese'); -- { serverError BAD_ARGUMENTS }

-- Behaviour of the other text-index functions on a `chinese` index. `hasAnyTokens` /
-- `hasAllTokens` tokenize the needle with the index tokenizer and match the segmented
-- words; `hasToken`, `has` (equals) and `match` use their own (splitByNonAlpha / regexp)
-- semantics and are unaffected by the chinese segmentation.
SELECT '-- other functions on a chinese text index';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = 'chinese') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO tab VALUES (1, '他来到了网易杭研大厦'), (2, '我来自北京邮电大学');

SELECT '-- hasAllTokens / hasAnyTokens match segmented words';
SELECT id FROM tab WHERE hasAllTokens(doc, ['网易']) ORDER BY id;
SELECT id FROM tab WHERE hasAnyTokens(doc, ['北京邮电大学']) ORDER BY id;

SELECT '-- hasToken uses splitByNonAlpha: a CJK sub-word is never a whole token, so 0 rows';
SELECT count() FROM tab WHERE hasToken(doc, '网易');
-- and it agrees with the no-index evaluation
SELECT count() FROM tab WHERE hasToken(doc, '网易') SETTINGS use_skip_indexes = 0;

SELECT '-- has (equals) matches the whole value only';
SELECT id FROM tab WHERE has([doc], '我来自北京邮电大学') ORDER BY id;

SELECT '-- match (regexp) is independent of tokenization';
SELECT id FROM tab WHERE match(doc, '北京邮电') ORDER BY id;
SELECT id FROM tab WHERE match(doc, '清华') ORDER BY id;

DROP TABLE tab;
