-- Test: highlight() function

-- Basic functionality
SELECT '-- Basic';
SELECT highlight('Hello World', ['hello']);
SELECT highlight('The quick brown fox', ['quick', 'fox']);
SELECT highlight('Hello World', ['xyz']);
SELECT highlight('hello', ['hello']);

-- Case insensitivity (ASCII)
SELECT '-- Case insensitive';
SELECT highlight('HELLO hello HeLLo', ['hello']);
SELECT highlight('Hello WORLD', ['hello', 'world']);

-- Overlapping and adjacent intervals
SELECT '-- Overlapping';
SELECT highlight('abcdef', ['abc', 'cde']);
SELECT highlight('abcdef', ['abc', 'def']);
SELECT highlight('foobar', ['foo', 'foobar']);
SELECT highlight('aaaaaa', ['aaaa']);

-- Edge cases
SELECT '-- Edge cases';
SELECT highlight('Hello', []::Array(String)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT highlight('', ['hello']);
SELECT highlight('Hello', ['', 'hello']);

-- Custom tags
SELECT '-- Custom tags';
SELECT highlight('Hello World', ['hello'], '<b>', '</b>');
SELECT highlight('Hello World', ['hello'], '', '');
SELECT highlight('text here', ['text'], '<span class="hl">', '</span>');

-- UTF-8 text
SELECT '-- UTF-8';
SELECT highlight('Привет мир', ['Привет']);
SELECT highlight('Hello Мир', ['hello']);
SELECT highlight('körtefa', ['kÖrte']);  -- non-ASCII case: no match expected

-- Column input
SELECT '-- Column input';
SELECT highlight(s, ['a']) FROM (SELECT arrayJoin(['abc', 'def', 'gha']) AS s);

-- Multiple matches of same term
SELECT '-- Multiple matches';
SELECT highlight('cat and cat and cat', ['cat']);

-- FixedString input
SELECT '-- FixedString';
SELECT highlight(toFixedString('Hello World', 20), ['hello']);
SELECT highlight(toFixedString('abc', 5), ['abc']);  -- trailing padding stripped
