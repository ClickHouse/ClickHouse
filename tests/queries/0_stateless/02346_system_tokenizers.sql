-- Tags: no-fasttest
-- no-fasttest: the 'icu' tokenizer is only registered when built with ICU

SELECT * FROM system.tokenizers ORDER BY ALL;
