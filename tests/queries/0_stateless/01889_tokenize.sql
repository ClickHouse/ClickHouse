SET allow_experimental_nlp_functions = 1;

SELECT splitByNonAlpha('It is quite a wonderful day, isn\'t it?');
SELECT splitByNonAlpha('There is.... so much to learn!');
SELECT splitByNonAlpha('22:00 email@yandex.ru');
SELECT splitByNonAlpha('Токенизация каких-либо других языков?');

SELECT splitByWhitespace('It is quite a wonderful day, isn\'t it?');
SELECT splitByWhitespace('There is.... so much to learn!');
SELECT splitByWhitespace('22:00 email@yandex.ru');
SELECT splitByWhitespace('Токенизация каких-либо других языков?');
