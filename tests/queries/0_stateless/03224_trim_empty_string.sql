SELECT trim(LEADING '' FROM 'foo');
SELECT trim(TRAILING '' FROM 'foo');
SELECT trim(BOTH '' FROM 'foo');

SELECT trim(LEADING '' FROM ' foo ') FORMAT CSV;
SELECT trim(TRAILING '' FROM ' foo ') FORMAT CSV;
SELECT trim(BOTH '' FROM ' foo ') FORMAT CSV;
