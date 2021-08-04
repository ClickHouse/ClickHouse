DROP TABLE IF EXISTS one_hot_source;
CREATE TABLE one_hot_source (Id String, X String, Y String, Z String) Engine=Memory;
INSERT INTO TABLE one_hot_source VALUES (1, 'A','','c'),(2,'B','a','Y'),(3,'C','','c'),(4,'A','b','c'),(5,'D','c','d'),(6,'B','d','c');
-- use one_hot_encoding_view to encode X,Y,Z, and Id columns
SELECT * FROM one_hot_encoding_view((SELECT * FROM one_hot_source),X,Y,Z,Id);
-- check for basic SQL injection
SELECT * FROM one_hot_encoding_view((SELECT * FROM one_hot_source),`SHOW TABLES`); -- {serverError 36}
-- check when base query is empty
SELECT * FROM one_hot_encoding_view((),`SHOW TABLES`); -- { clientError 62} 
-- check dummy base query
SELECT * FROM one_hot_encoding_view((SELECT 1),`SHOW TABLES`); -- {serverError 36}
-- check dummy base query with valid colum
SELECT * FROM one_hot_encoding_view((SELECT 1),`1`);
DROP TABLE one_hot_source;
