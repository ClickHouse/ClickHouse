SELECT translate('Hello? world.', '.?', '!,');
SELECT translate('gtcttgcaag', 'ACGTacgt', 'TGCAtgca');
SELECT translate(toString(number), '0123456789', 'abcdefghij') FROM numbers(987654, 5);

SELECT translateUTF8('HôtelGenèv', 'Ááéíóúôè', 'aaeiouoe');
SELECT translateUTF8('中文内码', '久标准中文内码', 'ユニコードとは');
SELECT translateUTF8(toString(number), '1234567890', 'ዩय𐑿𐐏নՅðй¿ค') FROM numbers(987654, 5);

SELECT translate('abc', '', '');
SELECT translateUTF8('abc', '', '');

SELECT translate('abc', 'Ááéíóúôè', 'aaeiouoe'); -- { serverError 36 }
SELECT translateUTF8('abc', 'efg', ''); -- { serverError 36 }
