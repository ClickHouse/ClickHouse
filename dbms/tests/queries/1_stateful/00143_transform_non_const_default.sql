SELECT transform(SearchEngineID, [2, 3], ['Яндекс', 'Google'], PageCharset) AS title, count() AS c FROM test.hits WHERE SearchEngineID != 0 GROUP BY title HAVING c > 0 ORDER BY c DESC LIMIT 10;
