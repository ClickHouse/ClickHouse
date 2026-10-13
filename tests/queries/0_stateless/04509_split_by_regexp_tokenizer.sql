-- Detailed test of the `splitByRegexp` tokenizer through the `tokens` function.
-- The regular expression plays the role of the separator; the tokens are the (non-empty) pieces of text
-- between successive matches. With `match_tokens` set to true (or any nonzero integer), this is
-- reversed: `regexp` matches the tokens themselves (capture group 1, or the whole match if it has none).

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
-- `match_tokens` must be a const Bool or an unsigned integer (see positive tests below)
SELECT tokens('a', 'splitByRegexp', 'a', 'x'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tokens('a', 'splitByRegexp', 'a', materialize(true)); -- { serverError ILLEGAL_COLUMN }
-- A signed integer is rejected too, not silently misrouted into the Array(String) branch
SELECT tokens('a', 'splitByRegexp', 'a', toInt8(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Too many arguments
SELECT tokens('a', 'splitByRegexp', 'a', 1, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- With match_tokens = true, a pattern that can match an empty string is rejected outright: nextMatchedToken
-- has no ordinary-RE2-semantics way to skip an empty match without either getting stuck or silently
-- changing which alternative wins for unrelated, non-empty matches (see the positive tests below)
SELECT tokens('abc', 'splitByRegexp', 'z*', true); -- { serverError BAD_ARGUMENTS }
SELECT tokens('123x45', 'splitByRegexp', '[0-9]*', true); -- { serverError BAD_ARGUMENTS }
SELECT tokens('a', 'splitByRegexp', '|a', true); -- { serverError BAD_ARGUMENTS }
SELECT tokens('y', 'splitByRegexp', 'x?|y', true); -- { serverError BAD_ARGUMENTS }

-- { echoOn }
-- Simple literal separator
SELECT tokens('a,b,c', 'splitByRegexp', ',');
-- A capture group in the separator is inert in the default mode (captures are tracked internally
-- regardless of mode, but only read when match_tokens = true)
SELECT tokens('a,b,c', 'splitByRegexp', '(,)');
-- The default mode keeps RE2's leftmost-first semantics: the first-listed alternative wins even if a
-- later one would match more, so 'a' is the separator here, not 'ab'
SELECT tokens('ab', 'splitByRegexp', 'a|ab');
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

-- match_tokens = 1: capture group 1 becomes the token; the surrounding context ('tag:') is discarded
SELECT tokens('tag:hello tag:world', 'splitByRegexp', 'tag:(\\w+)', 1);
-- true is equivalent to 1, and is the recommended, self-documenting form
SELECT tokens('tag:hello tag:world', 'splitByRegexp', 'tag:(\\w+)', true) = tokens('tag:hello tag:world', 'splitByRegexp', 'tag:(\\w+)', 1);
-- Any nonzero integer is truthy, like other Bool-documented arguments that accept 0/1
SELECT tokens('a', 'splitByRegexp', 'a', 2);
-- No capture groups: falls back to the whole RE2 match
SELECT tokens('a1b22c333', 'splitByRegexp', '[0-9]+', 1);
-- Contrast with match_tokens = 0 (separator mode) on the same pattern: the text *between* matches
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
-- A pattern with a capture group is never "trivial", so the substring-search fast path never applies
SELECT tokens('abc', 'splitByRegexp', 'a(b)c', 1);
-- Ordered alternatives keep ordinary RE2 (leftmost-first) semantics, same as separator mode: 'a' wins
-- over the longer 'ab', it is not the other way around
SELECT tokens('ab', 'splitByRegexp', '(a|ab)', true);
-- Same as above, but each alternative has its own capture group: group 1 ('a') still wins, since the
-- first alternative that matches at a position is taken regardless of what a later one could capture
SELECT tokens('ab', 'splitByRegexp', '(a)|(ab)', true);
-- Multi-byte UTF-8 is preserved inside the capture group (byte offsets must not slice a character)
SELECT tokens('k:héllo k:wörld', 'splitByRegexp', 'k:(\\p{L}+)', 1);
-- `\w` is ASCII-only in RE2, so it stops at the first multi-byte character
SELECT tokens('k:héllo', 'splitByRegexp', 'k:(\\w+)', 1);
-- Result type and constness with match_tokens = 1
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

SELECT 'Column values with match_tokens = 1: tokens should be non-constant';

CREATE TABLE tab (
    id Int64,
    str String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO tab (id, str) VALUES (1, 'tag:red tag:green'), (2, 'tag:blue'), (3, 'no tags here');

SELECT id, tokens(str, 'splitByRegexp', 'tag:(\\w+)', 1) AS tokenized, isConstant(tokenized) FROM tab ORDER BY id;

DROP TABLE tab;
