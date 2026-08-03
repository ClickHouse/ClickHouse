-- Tags: no-fasttest
-- no-fasttest: the 'icu' tokenizer is only registered when built with ICU (ICU is off only in the FastTest build)
-- The 'japanese' tokenizer is only registered when built with MeCab; exclude it for a build-independent result.
SELECT * FROM system.tokenizers WHERE name != 'japanese' ORDER BY ALL;
