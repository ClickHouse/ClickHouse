-- Tags: no-fasttest
-- no-fasttest: the `chinese` tokenizer is only built with jieba (cppjieba), which is not
-- available in the fast test build, so this test (which lists all tokenizers including
-- `chinese`) is excluded there.

SELECT * FROM system.tokenizers ORDER BY ALL;
