-- Tags: no-fasttest
-- no-fasttest because aklomp-base64 library is required

-- incorrect number of arguments
SELECT base64URLEncode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base64URLDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tryBase64URLDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base64URLEncode('foo', 'excess argument'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base64URLDecode('foo', 'excess argument'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tryBase64URLDecode('foo', 'excess argument'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- test with valid inputs

SELECT 'https://clickhouse.com' AS original, base64URLEncode(original) AS encoded, base64URLDecode(encoded), tryBase64URLDecode(encoded);
SELECT '12?' AS original, base64URLEncode(original) AS encoded, base64URLDecode(encoded), tryBase64URLDecode(encoded);
SELECT 'https://www.google.com/search?q=clickhouse+base64+decode&sca_esv=739f8bb380e4c7ed&ei=TfRiZqCDIrmnwPAP2KLRkA8&ved=0ahUKEwjg3ZHitsmGAxW5ExAIHVhRFPIQ4dUDCBA&uact=5&oq=clickhouse+base64+decode' AS original, base64URLEncode(original) AS encoded, base64URLDecode(encoded), tryBase64URLDecode(encoded);

-- encoded value has no padding
SELECT 'aHR0cHM6Ly9jbGlj' AS encoded, base64URLDecode(encoded), tryBase64URLDecode(encoded);
-- encoded value has one-byte padding
SELECT 'aHR0cHM6Ly9jbGlja2g' AS encoded, base64URLDecode(encoded), tryBase64URLDecode(encoded);
-- encoded value has two-bytes padding
SELECT 'aHR0cHM6Ly9jbGljaw' AS encoded, base64URLDecode(encoded), tryBase64URLDecode(encoded);

-- test with invalid inputs

SELECT base64URLDecode('https://clickhouse.com'); -- { serverError INCORRECT_DATA }
SELECT tryBase64URLDecode('https://clickhouse.com');
SELECT base64URLDecode('12?'); -- { serverError INCORRECT_DATA }
SELECT tryBase64URLDecode('12?');
SELECT base64URLDecode('aHR0cHM6Ly9jbGlja'); -- { serverError INCORRECT_DATA }
SELECT tryBase64URLDecode('aHR0cHM6Ly9jbGlja');

-- test FixedString argument

SELECT toFixedString('https://clickhouse.com', 22) AS original, base64URLEncode(original) AS encoded, base64URLDecode(encoded), tryBase64URLDecode(encoded);
