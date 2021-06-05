SELECT tokenize('It is quite a wonderful day, isn\'t it?');
SELECT tokenize('There is.... so much to learn!');
SELECT tokenize('22:00 email@yandex.ru');
SELECT tokenize('Токенизация каких-либо других языков?');

SELECT tokenizeWhitespace('It is quite a wonderful day, isn\'t it?');
SELECT tokenizeWhitespace('There is.... so much to learn!');
SELECT tokenizeWhitespace('22:00 email@yandex.ru');
SELECT tokenizeWhitespace('Токенизация каких-либо других языков?');
