-- Tags: no-fasttest

SET allow_experimental_nlp_functions = 1;

SELECT lemmatize('en', 'wolves');
SELECT lemmatize('en', 'dogs');
SELECT lemmatize('en', 'looking');
SELECT lemmatize('en', 'took');
SELECT lemmatize('en', 'imported');
SELECT lemmatize('en', 'tokenized');
SELECT lemmatize('en', 'flown');

SELECT synonyms('en', 'crucial');
SELECT synonyms('en', 'cheerful');
SELECT synonyms('en', 'yet');
SELECT synonyms('en', 'quiz');
SELECT synonyms('ru', 'главный');
SELECT synonyms('ru', 'веселый');
SELECT synonyms('ru', 'правда');
SELECT synonyms('ru', 'экзамен');
