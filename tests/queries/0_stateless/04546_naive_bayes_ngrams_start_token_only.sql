-- The 4-argument form: a start token is supplied but the end token is omitted, so padding is applied only
-- at the start of the input. It is equivalent to the 5-argument form with an empty end token.

SELECT naiveBayesNgrams('cat', 2, 'token', '<s>');
SELECT arrayMap(x -> hex(x), naiveBayesNgrams('xy', 2, 'byte', '0x01'));
SELECT arrayMap(x -> hex(x), naiveBayesNgrams('ab', 2, 'codepoint', '0x5E'));
SELECT naiveBayesNgrams('xy', 2, 'byte', '0x01') = naiveBayesNgrams('xy', 2, 'byte', '0x01', '');
SELECT naiveBayesNgrams('xy', 2, 'byte', '') = naiveBayesNgrams('xy', 2, 'byte');
