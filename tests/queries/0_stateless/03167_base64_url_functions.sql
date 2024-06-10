-- incorrect number of arguments
SELECT base64UrlEncode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base64UrlDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tryBase64UrlDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base64UrlEncode('foo', 'excess argument'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base64UrlDecode('foo', 'excess argument'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tryBase64UrlDecode('foo', 'excess argument'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- test with valid inputs
SELECT 'https://clickhouse.com' as original, base64UrlEncode(original) as encoded, base64UrlDecode(encoded) as decoded, tryBase64UrlDecode(encoded) as try_decoded;

-- encoding differs from base64Encode
SELECT '12?' as original, base64UrlEncode(original) as encoded, base64UrlDecode(encoded) as decoded, tryBase64UrlDecode(encoded) as try_decoded;

-- long string
SELECT 'https://www.google.com/search?q=clickhouse+base64+decode&sca_esv=739f8bb380e4c7ed&ei=TfRiZqCDIrmnwPAP2KLRkA8&ved=0ahUKEwjg3ZHitsmGAxW5ExAIHVhRFPIQ4dUDCBA&uact=5&oq=clickhouse+base64+decode' as original, base64UrlEncode(original) as encoded, base64UrlDecode(encoded) as decoded, tryBase64UrlDecode(encoded) as try_decoded;

-- no padding
SELECT 'aHR0cHM6Ly9jbGlj' as encoded, base64UrlDecode(encoded) as decoded, tryBase64UrlDecode(encoded) as try_decoded;
-- one-byte padding
SELECT 'aHR0cHM6Ly9jbGlja2g' as encoded, base64UrlDecode(encoded) as decoded, tryBase64UrlDecode(encoded) as try_decoded;
-- two-bytes padding
SELECT 'aHR0cHM6Ly9jbGljaw' as encoded, base64UrlDecode(encoded) as decoded, tryBase64UrlDecode(encoded) as try_decoded;

-- invalid inputs
SELECT base64UrlDecode('https://clickhouse.com'); -- { serverError INCORRECT_DATA }
SELECT tryBase64UrlDecode('https://clickhouse.com');
SELECT base64UrlDecode('12?'); -- { serverError INCORRECT_DATA }
SELECT tryBase64UrlDecode('12?');
SELECT base64UrlDecode('aHR0cHM6Ly9jbGlja'); -- { serverError INCORRECT_DATA }
SELECT tryBase64UrlDecode('aHR0cHM6Ly9jbGlja');

-- test FixedString argument
SELECT toFixedString('https://clickhouse.com', 22) as original, base64UrlEncode(original) as encoded, base64UrlDecode(encoded) as decoded, tryBase64UrlDecode(encoded) as try_decoded;
