-- Detailed test of the `splitByRegexp` tokenizer through the `tokens` function.
-- The regular expression plays the role of the separator; the tokens are the (non-empty) pieces of text
-- between successive matches.

SELECT 'Negative tests';
-- The regular expression argument is mandatory
SELECT tokens('a', 'splitByRegexp'); -- { serverError BAD_ARGUMENTS }
-- and must be a const String
SELECT tokens('a', 'splitByRegexp', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'splitByRegexp', toFixedString('c', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'splitByRegexp', ['c']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'splitByRegexp', materialize('c')); -- { serverError ILLEGAL_COLUMN }
-- and must not be empty
SELECT tokens('a', 'splitByRegexp', ''); -- { serverError BAD_ARGUMENTS }

-- { echoOn }
-- Simple literal separator
SELECT tokens('a,b,c', 'splitByRegexp', ',');
-- Leading, trailing and consecutive separators do not produce empty tokens
SELECT tokens(',a,,b,', 'splitByRegexp', ',');
-- No match: the whole string is a single token
SELECT tokens('abc', 'splitByRegexp', ',');
-- Only separators: no tokens
SELECT tokens(',,,', 'splitByRegexp', ',');
-- Empty input: no tokens
SELECT tokens('', 'splitByRegexp', ',');
-- Whitespace run
SELECT tokens('a  b\tc\n\nd', 'splitByRegexp', '\\s+');
-- Character class
SELECT tokens('a,b;c', 'splitByRegexp', '[,;]');
-- One-or-more quantifier
SELECT tokens('a--b---c', 'splitByRegexp', '-+');
-- Digit run
SELECT tokens('a1b22c333', 'splitByRegexp', '[0-9]+');
-- Negated character class: digits, spaces and upper-case letters are separators, so 'BAZ' is consumed
SELECT tokens('foo123bar BAZ qux', 'splitByRegexp', '[^a-z]+');
-- Multi-character literal separator
SELECT tokens('a::b::c', 'splitByRegexp', '::');
-- Alternation of literals
SELECT tokens('xandyorz', 'splitByRegexp', 'and|or');
-- Unescaped dot matches any character, so every position is a separator
SELECT tokens('abc', 'splitByRegexp', '.');
-- Escaped dot is a literal separator
SELECT tokens('a.b.c', 'splitByRegexp', '\\.');
-- A separator that can only match empty never splits
SELECT tokens('abc', 'splitByRegexp', 'z*');
-- Multi-byte UTF-8 content is preserved
SELECT tokens('héllo,wörld', 'splitByRegexp', ',');
-- Multi-byte UTF-8 separator
SELECT tokens('a→b→c', 'splitByRegexp', '→');
-- The ^ anchor matches only at the true start of the string
SELECT tokens('aba', 'splitByRegexp', '^a');
-- The $ anchor matches only at the true end of the string
SELECT tokens('banana', 'splitByRegexp', 'a$');
-- A letter as a separator splits around every occurrence
SELECT tokens('banana', 'splitByRegexp', 'a');
-- Matching is case-sensitive: only the upper-case X splits
SELECT tokens('aXbxc', 'splitByRegexp', 'X');
-- Non-word characters as separator (\W keeps '_' as a word character)
SELECT tokens('the_lazy dog-cat', 'splitByRegexp', '\\W+');
-- Issue #103783: keep special-character tokens such as C++ and C# (separator is any run of characters
-- that are not letters, digits, '#' or '+'), which splitByNonAlpha would otherwise reduce to 'c'
SELECT tokens('We use C++ for our backend systems', 'splitByRegexp', '[^\\p{L}\\p{N}#+]+');
SELECT tokens('Built with C# and React', 'splitByRegexp', '[^\\p{L}\\p{N}#+]+');
-- Result type and constness
SELECT tokens('a,b,c', 'splitByRegexp', ',') AS tokenized, toTypeName(tokenized), isConstant(tokenized);
-- { echoOff }

SELECT 'Column values: tokens should be non-constant';

CREATE TABLE tab (
    id Int64,
    str String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO tab (id, str) VALUES (1, 'a1b2c3'), (2, 'foo42bar'), (3, 'x,y,,z');

SELECT id, tokens(str, 'splitByRegexp', '[0-9,]+') AS tokenized, isConstant(tokenized) FROM tab ORDER BY id;

DROP TABLE tab;
