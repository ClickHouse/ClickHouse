-- Tags: stateful
SELECT count()
FROM
(
    SELECT DISTINCT
        Title,
        SearchPhrase
    FROM test.hits
    WHERE (SearchPhrase != '') AND (NOT match(Title, '[а-яА-ЯёЁ]')) AND (NOT match(SearchPhrase, '[а-яА-ЯёЁ]'))
    LIMIT 1 BY Title
    LIMIT 10
);
