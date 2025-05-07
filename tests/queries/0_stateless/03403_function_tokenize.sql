-- Tags: no-fasttest

SELECT 'Default tokenizer.';

WITH 'abc def foo! bar.' as text
SELECT tokenize('default', text) as tokenized, length(tokenized) as length;
WITH 'abc+ def- foo! bar? baz= code; hello: world/' as text
SELECT tokenize('default', text) as tokenized, length(tokenized) as length;

SELECT 'Ngram tokenizer.';

WITH 'abc def' as text
SELECT tokenize('ngram', 1, text) as tokenized, length(tokenized) as length;

WITH 'abc def' as text
SELECT tokenize('ngram', 3, text) as tokenized, length(tokenized) as length;

WITH 'abc def' as text
SELECT tokenize('ngram', 100, text) as tokenized, length(tokenized) as length;

SELECT 'NoOp tokenizer.';

WITH 'abc def' as text
SELECT tokenize('noop', text) as tokenized, length(tokenized) as length;
