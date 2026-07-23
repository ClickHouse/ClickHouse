-- The 'japanese' tokenizer is only registered when built with MeCab; exclude it for a build-independent result.
SELECT * FROM system.tokenizers WHERE name != 'japanese' ORDER BY ALL;
