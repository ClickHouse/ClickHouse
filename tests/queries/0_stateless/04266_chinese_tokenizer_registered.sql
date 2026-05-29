-- Tags: no-fasttest
-- Tag no-fasttest: jieba (Chinese tokenizer) is not built in fast test.

-- The `chinese` tokenizer is only registered in builds with jieba (USE_JIEBA).
-- The general `system.tokenizers` coverage lives in `02346_system_tokenizers`, which
-- stays fasttest-compatible by excluding this tokenizer.

SELECT name FROM system.tokenizers WHERE name = 'chinese';
