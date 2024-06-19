-- Tags: no-fasttest
-- no-fasttest because aklomp-base64 library is required

-- incorrect number of arguments
SELECT base64UrlEncode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base64UrlDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tryBase64UrlDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base64UrlEncode('foo', 'excess argument'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base64UrlDecode('foo', 'excess argument'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tryBase64UrlDecode('foo', 'excess argument'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- test with valid inputs

SELECT 'https://clickhouse.com' AS original, base64UrlEncode(original) AS encoded, base64UrlDecode(encoded), tryBase64UrlDecode(encoded);
SELECT '12?' AS original, base64UrlEncode(original) AS encoded, base64UrlDecode(encoded), tryBase64UrlDecode(encoded);
SELECT 'https://www.google.com/search?q=clickhouse+base64+decode&sca_esv=739f8bb380e4c7ed&ei=TfRiZqCDIrmnwPAP2KLRkA8&ved=0ahUKEwjg3ZHitsmGAxW5ExAIHVhRFPIQ4dUDCBA&uact=5&oq=clickhouse+base64+decode' AS original, base64UrlEncode(original) AS encoded, base64UrlDecode(encoded), tryBase64UrlDecode(encoded);

-- encoded value has no padding
SELECT 'aHR0cHM6Ly9jbGlj' AS encoded, base64UrlDecode(encoded), tryBase64UrlDecode(encoded);
-- encoded value has one-byte padding
SELECT 'aHR0cHM6Ly9jbGlja2g' AS encoded, base64UrlDecode(encoded), tryBase64UrlDecode(encoded);
-- encoded value has two-bytes padding
SELECT 'aHR0cHM6Ly9jbGljaw' AS encoded, base64UrlDecode(encoded), tryBase64UrlDecode(encoded);

-- test with invalid inputs

SELECT base64UrlDecode('https://clickhouse.com'); -- { serverError INCORRECT_DATA }
SELECT tryBase64UrlDecode('https://clickhouse.com');
SELECT base64UrlDecode('12?'); -- { serverError INCORRECT_DATA }
SELECT tryBase64UrlDecode('12?');
SELECT base64UrlDecode('aHR0cHM6Ly9jbGlja'); -- { serverError INCORRECT_DATA }
SELECT tryBase64UrlDecode('aHR0cHM6Ly9jbGlja');

-- test FixedString argument

SELECT toFixedString('https://clickhouse.com', 22) AS original, base64UrlEncode(original) AS encoded, base64UrlDecode(encoded), tryBase64UrlDecode(encoded);
