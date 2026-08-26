-- Detailed test of the `splitByRegexp` tokenizer through the `tokens` function.
-- The regular expression plays the role of the separator; the tokens are the (non-empty) pieces of text
-- between successive matches. With the optional `extract` argument set to true (or any nonzero integer,
-- for consistency with how other Bool-documented arguments accept traditional 0/1 - see
-- checkAndGetLiteralArgument<bool>), this is reversed: `re` matches the tokens themselves (capture group
-- 1 of each match, or the whole match if `re` has no capture groups), and everything outside the matches
-- is discarded.

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
-- The `extract` argument must be a const Bool (or an integer, treated like other Bool-documented
-- arguments as truthy/falsy - see the positive tests below); a non-numeric type is rejected
SELECT tokens('a', 'splitByRegexp', 'a', 'x'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'splitByRegexp', 'a', materialize(true)); -- { serverError ILLEGAL_COLUMN }
-- Too many arguments
SELECT tokens('a', 'splitByRegexp', 'a', 1, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

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

-- extract = 1: capture group 1 becomes the token; the surrounding context ('tag:') is discarded
SELECT tokens('tag:hello tag:world', 'splitByRegexp', 'tag:(\\w+)', 1);
-- true is equivalent to 1, and is the recommended, self-documenting form
SELECT tokens('tag:hello tag:world', 'splitByRegexp', 'tag:(\\w+)', true) = tokens('tag:hello tag:world', 'splitByRegexp', 'tag:(\\w+)', 1);
-- Any nonzero integer is truthy, same as other Bool-documented arguments elsewhere (e.g. NPV's
-- start_from_zero) that traditionally accepted 0/1 before Bool existed
SELECT tokens('a', 'splitByRegexp', 'a', 2);
-- No capture groups: falls back to the whole RE2 match
SELECT tokens('a1b22c333', 'splitByRegexp', '[0-9]+', 1);
-- Contrast with extract = 0 (separator mode) on the same pattern: the text *between* matches
SELECT tokens('a1b22c333', 'splitByRegexp', '[0-9]+', 0);
-- A match whose capture group did not participate is skipped: only the 'foo' branch has a group,
-- so 'bar' matches contribute no token
SELECT tokens('foo bar foo bar', 'splitByRegexp', '(foo)|bar', 1);
-- A match whose capture group matched the empty string is skipped the same way
SELECT tokens('ac abc adc', 'splitByRegexp', 'a(b*)c', 1);
-- Scanning resumes after the *whole* match, not just after the captured span: if it resumed right
-- after the group, the pattern would find a second, overlapping match starting inside the first one
SELECT tokens('ababa', 'splitByRegexp', '(ab)a', 1);
-- Only the first capture group is used; further groups merely constrain what matches
SELECT tokens('k=v k2=v2', 'splitByRegexp', '(\\w+)=(\\w+)', 1);
-- A trivial literal pattern (no groups, served by a plain substring search rather than RE2) yields
-- the matches themselves
SELECT tokens('xabcyabcz', 'splitByRegexp', 'abc', 1);
-- A pattern with a capture group is never "trivial" - any `(` unconditionally takes the pattern off
-- the plain-substring-search fast path (OptimizedRegularExpression.cpp), so the fast path can never
-- see a capture group and skip populating it
SELECT tokens('abc', 'splitByRegexp', 'a(b)c', 1);
-- A pattern that can only match the empty string yields no tokens, since an empty match is not
-- treated as a match
SELECT tokens('abc', 'splitByRegexp', 'z*', 1);
-- A pattern that alternates between empty and non-empty matches does not stop scanning at the first
-- empty match: every later non-empty match is still found and extracted
SELECT tokens('123x45', 'splitByRegexp', '[0-9]*', 1);
-- Multi-byte UTF-8 is preserved inside the capture group (byte offsets must not slice a character)
SELECT tokens('k:héllo k:wörld', 'splitByRegexp', 'k:(\\p{L}+)', 1);
-- `\w` is ASCII-only in RE2, so it stops at the first multi-byte character
SELECT tokens('k:héllo', 'splitByRegexp', 'k:(\\w+)', 1);
-- Result type and constness with extract = 1
SELECT tokens('tag:hello', 'splitByRegexp', 'tag:(\\w+)', 1) AS tokenized, toTypeName(tokenized), isConstant(tokenized);
-- { echoOff }

SELECT 'Column values: tokens should be non-constant';

CREATE TABLE tab (
    id Int64,
    str String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO tab (id, str) VALUES (1, 'a1b2c3'), (2, 'foo42bar'), (3, 'x,y,,z');

SELECT id, tokens(str, 'splitByRegexp', '[0-9,]+') AS tokenized, isConstant(tokenized) FROM tab ORDER BY id;

DROP TABLE tab;

SELECT 'Column values with extract = 1: tokens should be non-constant';

CREATE TABLE tab (
    id Int64,
    str String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO tab (id, str) VALUES (1, 'tag:red tag:green'), (2, 'tag:blue'), (3, 'no tags here');

SELECT id, tokens(str, 'splitByRegexp', 'tag:(\\w+)', 1) AS tokenized, isConstant(tokenized) FROM tab ORDER BY id;

DROP TABLE tab;
