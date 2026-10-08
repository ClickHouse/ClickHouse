SELECT formatQuery('TRUNCATE TABLES FROM default LIKE \'%test%\'');
SELECT formatQuery('TRUNCATE TABLES FROM default ILIKE \'%test%\'');
SELECT formatQuery('TRUNCATE TABLES FROM default NOT LIKE \'%test%\'');
SELECT formatQuery('TRUNCATE TABLES FROM default NOT ILIKE \'%test%\'');
SELECT formatQuery('SHOW COLUMNS FROM tbl LIKE \'%a%\'');
SELECT formatQuery('SHOW COLUMNS FROM tbl ILIKE \'%a%\'');
SELECT formatQuery('SHOW COLUMNS FROM tbl NOT LIKE \'%a%\'');
SELECT formatQuery('SHOW COLUMNS FROM tbl NOT ILIKE \'%a%\'');
