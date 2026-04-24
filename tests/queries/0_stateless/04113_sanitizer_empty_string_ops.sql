-- Regression tests for sanitizer findings exposed by the `function_prop_fuzzer`
-- unit-test job (ASan+UBSan and MSan variants). See CIDB signatures:
--   MSan `protocol.h:17:9` in `getURLScheme`,
--   MSan `UTF8Encoding.cpp:156:10 Poco::UTF8Encoding::queryConvert`
--     (propagated from `UTF8::convertUTF8ToCodePoint` in `UTF8Helpers.cpp`).

-- `protocol` / `getURLScheme` on an empty URL previously dereferenced
-- `*data` with `size == 0`, reading one byte past the buffer.
SELECT protocol('');

-- `hasSubsequenceUTF8` calls `UTF8::convertUTF8ToCodePoint` with `size = end - pos`,
-- which may be zero at the string boundary. `convertUTF8ToCodePoint` forwards to
-- `Poco::UTF8Encoding::queryConvert` which reads the first byte without length check.
SELECT hasSubsequenceCaseInsensitiveUTF8('', '');
SELECT hasSubsequenceCaseInsensitiveUTF8('', 'x');
SELECT hasSubsequenceCaseInsensitiveUTF8('abc', '');
SELECT hasSubsequenceCaseInsensitiveUTF8('abc', 'ab');
SELECT hasSubsequenceCaseInsensitiveUTF8('abc', 'ac');
SELECT hasSubsequenceCaseInsensitiveUTF8('abc', 'd');

-- Non-ASCII to exercise the UTF-8 code-point path as well.
SELECT hasSubsequenceCaseInsensitiveUTF8('тест', '');
SELECT hasSubsequenceCaseInsensitiveUTF8('', 'тест');
