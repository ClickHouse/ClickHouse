-- Tags: no-fasttest
-- no-fasttest: the 'icu' tokenizer is only registered when built with ICU (ICU is off only in the FastTest build)
-- The 'chinese' and 'japanese' tokenizers depend on optional libraries (jieba/MeCab); exclude them for a build-independent result.
SELECT * FROM system.tokenizers WHERE name NOT IN ('chinese', 'japanese') ORDER BY ALL;
