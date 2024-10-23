SELECT sparseGrams('');
SELECT sparseGrams('a');
SELECT sparseGrams('ab');
SELECT sparseGrams('bce');
SELECT sparseGrams('abcdef');
SELECT sparseGrams('hello world');
SELECT sparseGrams('hello world hello world hello world hello world');

SELECT sparseGrams('', 5);
SELECT sparseGrams('cd', 5);
SELECT sparseGrams('hello world', 5);
SELECT sparseGrams('hello world hello world hello world hello world', 5);

SELECT sparseGramsUTF8('', 5);
SELECT sparseGramsUTF8('Клик', 5);
SELECT sparseGramsUTF8('Клик клак не тормозит', 5);

SELECT sparseGramsHashes('');
SELECT sparseGramsHashes('a');
SELECT sparseGramsHashes('ab');
SELECT sparseGramsHashes('bce');
SELECT sparseGramsHashes('abcdef');
SELECT sparseGramsHashes('hello world');
SELECT sparseGramsHashes('hello world hello world hello world hello world');

SELECT sparseGramsHashes('', 5);
SELECT sparseGramsHashes('cd', 5);
SELECT sparseGramsHashes('hello world', 5);
SELECT sparseGramsHashes('hello world hello world hello world hello world', 5);

SELECT sparseGramsHashesUTF8('', 5);
SELECT sparseGramsHashesUTF8('Клик', 5);
SELECT sparseGramsHashesUTF8('Клик клак не тормозит', 5);
