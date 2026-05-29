-- The `chinese` tokenizer is only available in builds with jieba (USE_JIEBA), so it is
-- excluded here to keep this test fasttest-compatible. Its registration is checked
-- separately in `04266_chinese_tokenizer_registered`.

SELECT * FROM system.tokenizers WHERE name != 'chinese' ORDER BY ALL;
