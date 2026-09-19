-- Tests the `asciiCJK_v2` tokenizer: the same tokens as `asciiCJK`, with the non-ASCII characters decoded by StringZilla.

-- { echoOn }

SELECT tokens('', 'asciiCJK_v2');
SELECT tokens('hello world', 'asciiCJK_v2', 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH, BAD_ARGUMENTS }
SELECT tokens(materialize('a:b a.3 3.14 don''t foo_bar _ __'), 'asciiCJK_v2');
SELECT tokens('中文，分词。café 한국어 テキスト 😀', 'asciiCJK_v2');
-- Invalid UTF-8: a stray continuation byte, a truncated sequence, an overlong encoding, a surrogate, a codepoint above
-- U+10FFFF and bytes that never start a sequence.
SELECT tokens('\x80a \xE4\xB8 \xC0\xAF \xED\xA0\x80 \xF4\x90\x80\x80 \xF8\xFF b\xE4', 'asciiCJK_v2');
SELECT tokensForLikePattern('%中文\\_分词%café%', 'asciiCJK_v2');

-- { echoOff }

SELECT 'Same tokens as asciiCJK';

SELECT countIf(tokens(s, 'asciiCJK_v2') != tokens(s, 'asciiCJK'))
FROM
(
    SELECT arrayJoin([
        '', 'hello world', 'a:b a.3 3.14 don''t foo_bar', '中文，分词。', 'café naïve', '한국어 テキスト', 'emoji 😀 👨‍👩‍👧',
        '\x80a \xE4\xB8 \xC0\xAF \xED\xA0\x80 \xF4\x90\x80\x80 \xF8\xFF b', '\xE4', '\xF0\x9F\x98', 'x\xC3']) AS s
);

-- Random bytes cover invalid and truncated UTF-8, random valid UTF-8 covers all codepoint lengths.
SELECT countIf(tokens(s, 'asciiCJK_v2') != tokens(s, 'asciiCJK')) FROM (SELECT randomString(number % 64) AS s FROM numbers(100000));
SELECT countIf(tokens(s, 'asciiCJK_v2') != tokens(s, 'asciiCJK')) FROM (SELECT randomStringUTF8(number % 64) AS s FROM numbers(100000));

SELECT countIf(tokensForLikePattern(s, 'asciiCJK_v2') != tokensForLikePattern(s, 'asciiCJK')) FROM (SELECT randomString(number % 64) AS s FROM numbers(100000));
SELECT countIf(tokensForLikePattern(s, 'asciiCJK_v2') != tokensForLikePattern(s, 'asciiCJK')) FROM (SELECT concat('%', randomStringUTF8(number % 64), '\\_', randomStringUTF8(number % 8), '%') AS s FROM numbers(100000));
