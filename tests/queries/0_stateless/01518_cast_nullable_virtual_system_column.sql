SELECT database FROM system.tables WHERE database LIKE '%' format Null;
SELECT database AS db FROM system.tables WHERE db LIKE '%' format Null;
SELECT CAST(database, 'String') AS db FROM system.tables WHERE db LIKE '%' format Null;
SELECT CAST('a string', 'Nullable(String)') AS str WHERE str LIKE '%' format Null;
SELECT CAST(database, 'Nullable(String)') AS ndb FROM system.tables WHERE ndb LIKE '%' format Null;
SELECT 'all tests passed';

