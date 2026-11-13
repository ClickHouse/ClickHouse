-- Tests that `tokens(s, 'ngrams', N)` handles UTF-8 sequence boundaries: a multi-byte sequence cut off
-- by the end of the string, invalid bytes, lone continuation bytes, and inputs with fewer code points
-- than N. Tokens are printed as hex so that the byte boundaries are visible.
-- Columns: the input (hex), then its ngrams for N = 1, 2, 3 and 8.

WITH ['', '61', '616263', '6162C3', '6162F09F98', 'C3', 'C3C3C3', 'C3A4C3B6', '808080', '61FF62',
      'F8F8F8', '65CC81', 'F09F9880F09F9881', '610062', 'C3A461',
      '6162636465666768696A6B6C6D6E6F707172737475767778797A30313233343536373839'] AS inputs
SELECT h,
       arrayMap(x -> hex(x), tokens(unhex(h), 'ngrams', 1)),
       arrayMap(x -> hex(x), tokens(unhex(h), 'ngrams', 2)),
       arrayMap(x -> hex(x), tokens(unhex(h), 'ngrams', 3)),
       arrayMap(x -> hex(x), tokens(unhex(h), 'ngrams', 8))
FROM (SELECT arrayJoin(inputs) AS h)
ORDER BY h;

-- A FixedString keeps its trailing NUL padding, and the padding bytes form ngrams of their own.
SELECT hex(x) FROM (SELECT arrayJoin(tokens(toFixedString('abc', 6), 'ngrams', 3)) AS x);

-- `hasAllTokens` finds every ngram of a string in the string itself, also when its last code point is cut off.
SELECT hasAllTokens(unhex('C3A4C3B6'), unhex('C3A4C3B6'), 'ngrams(2)');
SELECT hasAllTokens(unhex('65CC81'), unhex('65CC81'), 'ngrams(2)');
SELECT hasAllTokens(unhex('F09F9880F09F9881'), unhex('F09F9880F09F9881'), 'ngrams(2)');
SELECT hasAllTokens(unhex('C3A461'), unhex('C3A461'), 'ngrams(2)');
SELECT hasAllTokens(unhex('6162C3'), unhex('6162C3'), 'ngrams(3)');
SELECT hasAllTokens('hello world', 'hello world', 'ngrams(5)');
