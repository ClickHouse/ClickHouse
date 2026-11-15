-- Tags: no-fasttest
-- no-fasttest because simdutf library is required

-- Compatibility with the previous (aklomp-base64) implementation.

-- Non-zero leftover bits in a complete, properly padded final chunk are silently dropped.
SELECT base64Decode('Zh==');
SELECT tryBase64Decode('Zh==');
SELECT base64Decode('Zm9=');

-- The URL variant accepts an underpadded final chunk (the previous implementation padded the input
-- to a multiple of four characters before decoding).
SELECT base64URLDecode('Zg=');
SELECT tryBase64URLDecode('Zg=');
SELECT base64URLDecode('Zh=');
SELECT base64URLDecode('Zg =');

-- The standard variant still requires a complete, padded final chunk.
SELECT base64Decode('Zg='); -- { serverError INCORRECT_DATA }
SELECT tryBase64Decode('Zg=');
SELECT base64Decode('Zg'); -- { serverError INCORRECT_DATA }
SELECT tryBase64Decode('Zg');

-- Misplaced or excess padding is still rejected.
SELECT base64URLDecode('Zg=Zg='); -- { serverError INCORRECT_DATA }
SELECT tryBase64URLDecode('Zg=Zg=');
SELECT base64URLDecode('Zg==='); -- { serverError INCORRECT_DATA }
SELECT tryBase64URLDecode('Zg===');

-- Vertical tab is not among the ignored whitespace characters.
SELECT base64Decode('Zm9v\vYmFy'); -- { serverError INCORRECT_DATA }
SELECT tryBase64Decode('Zm9v\vYmFy');
SELECT base64URLDecode('Zm9v\vYmFy'); -- { serverError INCORRECT_DATA }
SELECT tryBase64URLDecode('Zm9v\vYmFy');
