SELECT count() FROM test.hits WHERE position(URL, 'metrika') != position(URL, materialize('metrika'));
SELECT count() FROM test.hits WHERE positionCaseInsensitive(URL, 'metrika') != positionCaseInsensitive(URL, materialize('metrika'));
SELECT count() FROM test.hits WHERE positionUTF8(Title, 'новости') != positionUTF8(Title, materialize('новости'));
SELECT count() FROM test.hits WHERE positionCaseInsensitiveUTF8(Title, 'новости') != positionCaseInsensitiveUTF8(Title, materialize('новости'));

SELECT position(URL, URLDomain) AS x FROM test.hits WHERE x = 0 AND URL NOT LIKE '%yandex.ru%' LIMIT 100;
SELECT URL FROM test.hits WHERE x > 10 ORDER BY position(URL, URLDomain) AS x DESC, URL LIMIT 2;
SELECT DISTINCT URL, URLDomain, position('http://yandex.ru/', URLDomain) AS x FROM test.hits WHERE x > 8 ORDER BY position('http://yandex.ru/', URLDomain) DESC, URL LIMIT 3;
