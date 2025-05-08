-- Tags: no-fasttest

SELECT 'Default tokenizer.';

WITH 'abc+ def- foo! bar? baz= code; hello: world/' as text
SELECT tokenize(text) as tokenized, length(tokenized) as length;
WITH 'abc def foo! bar.' as text
SELECT tokenize(text, 'default') as tokenized, length(tokenized) as length;
WITH 'abc+ def- foo! bar? baz= code; hello: world/' as text
SELECT tokenize(text, 'default') as tokenized, length(tokenized) as length;

SELECT 'Ngram tokenizer.';

WITH 'abc def' as text
SELECT tokenize(text, 'ngram', 0) as tokenized, length(tokenized) as length; -- { serverError BAD_ARGUMENTS }

WITH 'abc def' as text
SELECT tokenize(text, 'ngram', 1) as tokenized, length(tokenized) as length; -- { serverError BAD_ARGUMENTS }

WITH 'abc def' as text
SELECT tokenize(text, 'ngram', 3) as tokenized, length(tokenized) as length;

WITH 'abc def' as text
SELECT tokenize(text, 'ngram', 8) as tokenized, length(tokenized) as length;

WITH 'abc def' as text
SELECT tokenize(text, 'ngram', 100) as tokenized, length(tokenized) as length; -- { serverError BAD_ARGUMENTS }

SELECT 'NoOp tokenizer.';

WITH 'abc def' as text
SELECT tokenize(text, 'noop') as tokenized, length(tokenized) as length;
