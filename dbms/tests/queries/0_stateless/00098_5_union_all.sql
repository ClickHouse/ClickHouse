SELECT nn,vv FROM (SELECT name AS nn, value AS vv FROM data2013 UNION ALL SELECT name AS nn, value AS vv FROM data2014) ORDER BY nn,vv ASC;
