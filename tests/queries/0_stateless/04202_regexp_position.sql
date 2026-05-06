-- Tests for regexpPosition / regexpInstr / regexp_instr.

-- Basic usage: position of the first match.
SELECT regexpPosition('hello world', 'world');
SELECT regexpPosition('hello world', 'xyz');
SELECT regexpPosition('', 'a');
SELECT regexpPosition('abc', '');

-- start position
SELECT regexpPosition('aXbXcXd', 'X', 1);
SELECT regexpPosition('aXbXcXd', 'X', 3);
SELECT regexpPosition('aXbXcXd', 'X', 100);

-- occurrence
SELECT regexpPosition('aXbXcXd', 'X', 1, 1);
SELECT regexpPosition('aXbXcXd', 'X', 1, 2);
SELECT regexpPosition('aXbXcXd', 'X', 1, 3);
SELECT regexpPosition('aXbXcXd', 'X', 1, 4);

-- return_option: 0 = match start, 1 = position right after match
SELECT regexpPosition('hello world', 'world', 1, 1, 0);
SELECT regexpPosition('hello world', 'world', 1, 1, 1);
SELECT regexpPosition('aXbXcXd', 'X', 1, 2, 1);

-- flags
SELECT regexpPosition('Hello WORLD', 'world');
SELECT regexpPosition('Hello WORLD', 'world', 1, 1, 0, 'i');
SELECT regexpPosition('Hello WORLD', 'WORLD', 1, 1, 0, 'c');

-- subexpression
SELECT regexpPosition('foo123bar456', '([a-z]+)([0-9]+)', 1, 1, 0, '', 0);
SELECT regexpPosition('foo123bar456', '([a-z]+)([0-9]+)', 1, 1, 0, '', 1);
SELECT regexpPosition('foo123bar456', '([a-z]+)([0-9]+)', 1, 1, 0, '', 2);
SELECT regexpPosition('foo123bar456', '([a-z]+)([0-9]+)', 1, 2, 0, '', 2);
SELECT regexpPosition('foo123bar456', '([a-z]+)([0-9]+)', 1, 2, 1, '', 2);

-- Capture group that did not participate.
SELECT regexpPosition('abc', '(x)|(b)', 1, 1, 0, '', 1);
SELECT regexpPosition('abc', '(x)|(b)', 1, 1, 0, '', 2);

-- Aliases.
SELECT regexpInstr('hello world', 'world');
SELECT regexp_instr('hello world', 'world');
SELECT REGEXP_INSTR('hello world', 'WORLD', 1, 1, 0, 'i');

-- Vectorized over a column.
SELECT regexpPosition(s, 'b+') FROM (SELECT arrayJoin(['abc', 'aaabbb', 'xyz', 'bbbbbb']) AS s) ORDER BY s;

-- Constant haystack with vector numeric arguments (regression: previously read past end of const column).
SELECT regexpPosition('abc', 'a', number + 1) FROM numbers(3);
SELECT regexpPosition('aXbXcXd', 'X', 1, number + 1) FROM numbers(4);

-- Errors.
SELECT regexpPosition('abc', 'a', 0); -- { serverError BAD_ARGUMENTS }
SELECT regexpPosition('abc', 'a', 1, 0); -- { serverError BAD_ARGUMENTS }
SELECT regexpPosition('abc', 'a', 1, 1, 2); -- { serverError BAD_ARGUMENTS }
SELECT regexpPosition('abc', '(a)', 1, 1, 0, '', 5); -- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
SELECT regexpPosition('abc', 'a', 1, 1, 0, 'z'); -- { serverError BAD_ARGUMENTS }
SELECT regexpPosition('abc', materialize('a')); -- { serverError ILLEGAL_COLUMN }
