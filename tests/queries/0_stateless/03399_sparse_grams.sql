SELECT '--- Regular calls';
SELECT sparseGrams('');
SELECT sparseGrams('ab');
SELECT sparseGrams('bce');
SELECT sparseGrams('abcdef');
SELECT sparseGrams('hello world');
SELECT sparseGrams('hello world hello world');

SELECT '--- Minimal ngram length';
SELECT sparseGrams('', 5);
SELECT sparseGrams('hello world', 5);
SELECT sparseGrams('hello world hello world', 10);

SELECT '--- With UTF-8 chars';
SELECT sparseGramsUTF8('');
SELECT sparseGramsUTF8('a😊Ω𐍈界𝄞bЦ⛄');
SELECT sparseGramsUTF8('AΩЖ中😊🚀𝄞✨🎵🦄💡❄️', 4);

SELECT '--- Regular hashes';
SELECT sparseGramsHashes('');
SELECT sparseGramsHashes('ab');
SELECT sparseGramsHashes('bce');
SELECT sparseGramsHashes('abcdef');
SELECT sparseGramsHashes('hello world');
SELECT sparseGramsHashes('hello world hello world');

SELECT '--- Hashes with minimal ngram length';
SELECT sparseGramsHashes('', 5);
SELECT sparseGramsHashes('hello world', 5);
SELECT sparseGramsHashes('hello whole hello whole', 5);

SELECT '--- Hashes with UTF-8 strings';
SELECT sparseGramsHashesUTF8('');
SELECT sparseGramsHashesUTF8('a😊Ω𐍈界𝄞bЦ⛄');
SELECT sparseGramsHashesUTF8('AΩЖ中😊𝄞✨🌍🎵🦄💡❄️', 4);

SELECT '--- Check equal hashes';
WITH 'hello world hello world hello' as source
    SELECT sparseGramsHashes(source, 4) = arrayMap(v -> CRC32(v), sparseGrams(source, 4));
WITH 'AΩЖ中😊𝄞✨🌍🎵🦄💡❄️' as source
    SELECT sparseGramsHashesUTF8(source, 4) = arrayMap(v -> CRC32(v), sparseGramsUTF8(source, 4));
