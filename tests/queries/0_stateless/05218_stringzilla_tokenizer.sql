-- Tests the `stringzilla` tokenizer (UAX #29 word segmentation by StringZilla) with the `tokens` and
-- `tokensForLikePattern` functions.

-- { echoOn }

-- Return type and constness
SELECT tokens('', 'stringzilla') AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens('hello world', 'stringzilla') AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens(materialize('hello world'), 'stringzilla') AS tokenized, toTypeName(tokenized), isConstant(tokenized);
SELECT tokens(toNullable('hello world'), 'stringzilla') AS tokenized, toTypeName(tokenized);
SELECT tokens(toFixedString('hello world', 16), 'stringzilla') AS tokenized;
SELECT tokens('hello world', 'stringzilla', 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH, BAD_ARGUMENTS }

-- ASCII connectors follow UAX #29: `.` and `'` join letters or digits, `:` joins letters, `,` and `;` join digits
SELECT tokens('a_b a_3 a_ _a __a_b_3_ __', 'stringzilla');
SELECT tokens('a:b a:3 a: :a ::a:b:3:', 'stringzilla');
SELECT tokens($$a'b a'3 don't ''a'b'3'$$, 'stringzilla');
SELECT tokens('a.b a.3 3.14 U.S.A. v1.2.3', 'stringzilla');
SELECT tokens('1,000,000 1;2 12:30 e-mail user@example.com', 'stringzilla');

-- Separators: ASCII control whitespace, CRLF, whitespace runs, Unicode spaces, punctuation and symbols
SELECT tokens('tab\there\nnew\rcr\vvt\fff end', 'stringzilla');
SELECT tokens('line1\r\nline2\r\n\r\nline3', 'stringzilla');
SELECT tokens('  two   spaces  ', 'stringzilla');
SELECT tokens('nbsp\xC2\xA0ideographic\xE3\x80\x80space\xE2\x80\xA8ls', 'stringzilla');
SELECT tokens('price: $100, 50% off; x+y=z <tag> [a] {b} ~c', 'stringzilla');
SELECT tokens('\x00a\x01b\x7Fc', 'stringzilla');

-- CJK: ideographs and Hiragana are single-character tokens, Katakana and Hangul runs are words, CJK punctuation is dropped
SELECT tokens('中文分词测试，你好世界。', 'stringzilla');
SELECT tokens('日本語のテキスト カタカナ ひらがな「引用」', 'stringzilla');
SELECT tokens('한국어 텍스트', 'stringzilla');
SELECT tokens('错误503', 'stringzilla');
SELECT tokens('taichi张三丰in the house', 'stringzilla');
SELECT tokens('ClickHouse是一个用于OLAP的列式数据库', 'stringzilla');
SELECT tokens('中文，English；混合。', 'stringzilla');

-- Other scripts and combining marks
SELECT tokens('café naïve Ελληνικά русский', 'stringzilla');
SELECT tokens('e\xCC\x81te\xCC\x81 (decomposed)', 'stringzilla');
SELECT tokens('שלום עולם مرحبا بالعالم', 'stringzilla');

-- Emoji, emoji sequences, flags, and punctuation with combining marks are separators
SELECT tokens('emoji 😀 👍🏽 👨‍👩‍👧 ❤️ 🇺🇸🇫🇷 !\xCC\x81 ok', 'stringzilla');

-- Ill-formed UTF-8 is kept as tokens and never skips the following text
SELECT tokens('\xFF\xFE bad \xE4\xB8 trunc', 'stringzilla');
SELECT tokens('\xE4\xB8', 'stringzilla');

-- More tokens than one batch of segments
SELECT length(tokens(repeat('a b ', 1000), 'stringzilla')), length(tokens(repeat('中', 1000), 'stringzilla'));
SELECT tokens(repeat(' ', 1000) || 'x' || repeat('.', 1000), 'stringzilla');

-- tokensForLikePattern: a token next to a wildcard is dropped unless whitespace separates them, since the wildcard may continue it
SELECT tokensForLikePattern('', 'stringzilla');
SELECT tokensForLikePattern('%%__', 'stringzilla');
SELECT tokensForLikePattern('abc', 'stringzilla');
SELECT tokensForLikePattern('%abc', 'stringzilla');
SELECT tokensForLikePattern('abc%', 'stringzilla');
SELECT tokensForLikePattern('%abc def ghi%', 'stringzilla');
SELECT tokensForLikePattern('a_b%c_d', 'stringzilla');
SELECT tokensForLikePattern('_a c%', 'stringzilla');
SELECT tokensForLikePattern('%%__abc def', 'stringzilla');

-- `%.bar` may be `foo.bar`, `%,000` may be `1,000`
SELECT tokensForLikePattern('%.bar baz', 'stringzilla');
SELECT tokensForLikePattern('foo bar.%', 'stringzilla');
SELECT tokensForLikePattern($$foo bar'%$$, 'stringzilla');
SELECT tokensForLikePattern('%, 3 4,%', 'stringzilla');
SELECT tokensForLikePattern('%. bar baz .%', 'stringzilla');
SELECT tokensForLikePattern('%,000 rows%', 'stringzilla');
SELECT tokensForLikePattern('%1,000 rows,%', 'stringzilla');

-- Escaped wildcards and backslashes are literal characters
SELECT tokensForLikePattern('a\\%b a\\_c', 'stringzilla');
SELECT tokensForLikePattern('a\\_b%c', 'stringzilla');
SELECT tokensForLikePattern('a\\_b\\%c', 'stringzilla');
SELECT tokensForLikePattern('x \\_foo y', 'stringzilla');
SELECT tokensForLikePattern('x a\\\\%b y', 'stringzilla');
SELECT tokensForLikePattern('x y\\', 'stringzilla');
SELECT tokensForLikePattern('x caf\\é y', 'stringzilla');
SELECT tokensForLikePattern('%the quick brown\\_fox jumps over\\% the lazy dog%', 'stringzilla');

-- CJK
SELECT tokensForLikePattern('你', 'stringzilla');
SELECT tokensForLikePattern('abc你好', 'stringzilla');
SELECT tokensForLikePattern('%你好%世界%', 'stringzilla');
SELECT tokensForLikePattern('%错误502需要%', 'stringzilla');
SELECT tokensForLikePattern('%한국어 텍스트%', 'stringzilla');
