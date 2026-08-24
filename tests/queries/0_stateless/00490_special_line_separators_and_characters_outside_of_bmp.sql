-- Tags: no-fasttest

SELECT visitParamExtractString('{"x":"\\uD800\\udf38"}', 'x') AS x, visitParamExtractString('{"x":"Hello \\u2028 World \\u2029 !"}', 'x') AS y FORMAT JSONEachRow;
SELECT 'Hello' || convertCharset(unhex('2028'), 'utf16be', 'utf8') || 'World' || convertCharset(unhex('2029'), 'utf16be', 'utf8') || '!' AS x, hex(x) AS h FORMAT JSONEachRow;
SELECT 'Hello' || convertCharset(unhex('2028'), 'utf16be', 'utf8') || 'World' || convertCharset(unhex('2029'), 'utf16be', 'utf8') || '!' AS x, hex(x) AS h FORMAT TSV;
